from datetime import date

import polars as pl
import pytest

from scripts.audit_tw_futures_rule_coverage import coverage_table, expiry_worklist,expand_raw_universe


@pytest.mark.parametrize('problem',['damaged_unit','verified_mini','tampered_input'])
def test_numeric_rule_audit_separates_candidates_intervals_rates_and_source_identity(tmp_path,problem):
    from downloader.artifact_io import atomic_write_json,sha256_file
    from scripts.audit_tw_futures_rule_coverage import audit_numeric_rules
    root=tmp_path/'rules';root.mkdir()
    amount=79.5925 if problem=='verified_mini' else 2.
    tables=dict(
        corporate_event_candidates=pl.DataFrame([dict(product='AA1',effective_date='2020-01-02',
            contract_months=['202003'],contract_multiplier=amount,deliverable_security_quantity=amount,
            deliverable_cash_twd=0.,source_content_sha256='a'*64,issue_date_bound=True,
            extraction_method='native_cell_grid' if problem=='verified_mini' else 'source_ruled_ocr_cell_grid')]),
        corporate_unit_intervals=pl.DataFrame([dict(product='AA1',effective_date='2020-01-02',contract='202003',
            contract_multiplier=amount,source_content_sha256s=['a'*64])]),
        corporate_terms_intervals=pl.DataFrame(schema={'product':pl.String}),
        position_event_candidates=pl.DataFrame([dict(product='AAF',natural_person_limit=2000.,
            natural_person_monthly_limit=1000.,unit='contracts')]),
        position_level_intervals=pl.DataFrame([dict(product='DN2',unit='shares',position_unit=1.,
            event_type='combined_securities_position_formula',conversion_numerator=21,conversion_denominator=20)]),
        margin_event_candidates=pl.DataFrame([dict(product='AAF',margin_kind='notional_rate',
            after=[.135,.1035,.10],before=None)]),
        margin_level_intervals=pl.DataFrame(schema={'product':pl.String}))
    outputs={}
    for name,frame in tables.items():
        path=root/(name+'.parquet');frame.write_parquet(path);outputs[path.name]=dict(sha256=sha256_file(path))
    atomic_write_json(root/'manifest.json',dict(outputs=outputs))
    if problem=='tampered_input':
        (root/'corporate_event_candidates.parquet').write_bytes(b'corrupt after receipt')
        with pytest.raises(ValueError,match='SHA|hash|changed'):
            audit_numeric_rules(root,tmp_path/'audit')
        return
    assert audit_numeric_rules(root,tmp_path/'audit')==(0 if problem=='verified_mini' else 2)
    import json
    report=json.loads((tmp_path/'audit/manifest.json').read_text())
    assert report['source_review_interval_rows']==(0 if problem=='verified_mini' else 1)
    diagnostics=json.loads((tmp_path/'audit/numeric_rule_diagnostics.json').read_text())['diagnostics']
    assert all(row['table'].startswith('corporate_') for row in diagnostics)
    assert report['new_accounting_builds']==0 and not report['training_release_approved']


def frames():
    daily = pl.DataFrame({
        'date': [date(2011, 1, 3), date(2011, 1, 4), date(2011, 1, 3)],
        'product': ['TX', 'TX', 'DN2'], 'asset_class': ['index_future', 'index_future', 'stock_future'],
        'product_name': ['台指', '台指', '調整契約'],
        'physical_contract': ['TX-201101', 'TX-201101', 'DN2-201101'],
        'contract': ['201101'] * 3, 'contract_multiplier': [200., 200., 2000.],
        'liquidation_reason': ['carry_same_contract', 'last_trade_date', 'last_trade_date'],
        'settlement': [8000., 8100., 50.],
    })
    verified = daily.head(1).select('date', 'physical_contract')
    final = pl.DataFrame({'settlement_date': [date(2011, 1, 4)], 'product': ['TX'],
                          'contract': ['201101'], 'final_settlement_price': [8100.125]})
    return daily, verified, final


def test_terminal_numeric_audit_includes_already_valued_subscription_rights():
    from scripts.audit_tw_futures_rule_coverage import corporate_final_value_diagnostics
    terms=pl.DataFrame([dict(product='AA1',contract='202003',effective_date='2020-01-02',
        valid_until_exclusive=None,contract_multiplier=2000.,deliverable_cash_twd=0.,
        fixed_subscription_rights_twd=50.,subscription_rights_at_final_settlement=False)])
    final=pl.DataFrame([dict(product='AA1',contract='202003',settlement_date=date(2020,3,18),
        final_settlement_price=10.,final_settlement_value=20050.)])
    result=corporate_final_value_diagnostics(terms,final)
    assert result['candidate_formula_value_twd'][0]==20050.
    assert result['diagnostic_status'][0]=='within_rounding_envelope_not_rule_approval'


def test_terminal_diagnostic_does_not_infer_quantities_or_ignore_interval_ends():
    from scripts.audit_tw_futures_rule_coverage import corporate_final_value_diagnostics
    products=['AA1','BB1','CC1','DD1','EE1','FF1']
    terms=pl.DataFrame(dict(product=products,contract=['201309']*6,
        effective_date=['2013-08-01']*6,valid_until_exclusive=[None,None,'2013-09-01',None,None,None],
        contract_multiplier=[2060.,2.06,2060.,2060.,2060.,2060.],deliverable_cash_twd=[0.]*6,
        subscription_rights_at_final_settlement=[False]*4+[True,False]))
    final=pl.DataFrame(dict(product=products,contract=['201309']*6,
        settlement_date=[date(2013,9,18)]*6,final_settlement_price=[9.96]*6,
        final_settlement_value=[20517.,20517.,20517.,None,20599.,20600.]))
    out=corporate_final_value_diagnostics(terms,final)
    statuses=dict(out.select('product','diagnostic_status').iter_rows())
    assert statuses==dict(AA1='within_rounding_envelope_not_rule_approval',BB1='economic_value_conflict',
        CC1='candidate_term_ended_before_expiry',DD1='official_full_value_missing',
        EE1='rights_value_requires_separate_evidence',FF1='economic_value_conflict')
    assert not out['training_admitted'].any()
    assert out.filter(pl.col('product')=='BB1')['contract_multiplier'][0]==2.06
    assert out.filter(pl.col('product')=='DD1')['final_settlement_value'][0] is None
    with pytest.raises(ValueError,match='duplicate corporate'):
        corporate_final_value_diagnostics(pl.concat([terms,terms.head(1)]),final)


def test_candidate_chain_worklist_never_promotes_matching_or_missing_links():
    from scripts.audit_tw_futures_rule_coverage import margin_event_worklist
    base=dict(product='TX',margin_kind='fixed_twd',after=[100.,80.,70.],before=None,
        published_date='2020-01-01',known_at='2020-01-01T23:59:59+08:00',
        effective_date='2020-01-02',effective_phase='after_product_regular_close',
        source_content_sha256='a',source_url='a',issue_date_bound=True,chronological=True,
        requires_reversion_review=False)
    bad=dict(base,effective_date='2020-02-02',published_date='2020-02-01',
             known_at='2020-02-01T23:59:59+08:00',source_content_sha256='b',source_url='b',
             before=[90.,75.,65.],after=[110.,90.,80.])
    work=margin_event_worklist(pl.DataFrame([base,bad]))
    assert work.height==1 and work['reasons'][0]=='before_after_chain_disagrees'
    assert not work['training_admitted'].any()
    good=dict(bad,before=base['after'])
    assert margin_event_worklist(pl.DataFrame([base,good])).height==0
    conflict=dict(base,after=[120.,100.,90.],source_content_sha256='c',source_url='c')
    assert margin_event_worklist(pl.DataFrame([base,conflict]))['reasons'][0]=='same_boundary_conflicting_values'


def test_prepared_coverage_keeps_all_asset_classes_and_zero_fact_codes():
    from scripts.audit_tw_futures_rule_coverage import prepared_scope_coverage
    products=['TX','TGF','GBF','CJ2']
    universe=pl.DataFrame(dict(product=products,product_name=products,
        asset_class=['index_future','commodity_future','interest_rate_future','stock_future'],
        settlement_currency=['TWD']*4,underlying_symbol=['','','','2880'],
        source_start=['2011-01-01']*4,source_end=['2026-09-24']*4,source_rows=[3,2,1,1],
        needs_dated_deliverables=[False,False,False,True]))
    lives=pl.DataFrame(dict(product=products,lifetime_status=['official_final']*3+['unresolved_expiry'],
        settlement_method=['cash_settlement','cash_settlement','physical_delivery',None]))
    missing=pl.DataFrame({'product':['CJ2']})
    tables={k:pl.DataFrame({'product':['TX']}) for k in ('margin','position','corporate')}
    result=prepared_scope_coverage(universe,lives,missing,tables)
    assert set(result['product'])==set(products)
    assert not result['complete_rule_chain_verified'].any()
    assert result.filter(pl.col('product')=='CJ2')['candidate_margin_events'][0]==0
    assert result.filter(pl.col('product')=='GBF')['physical_delivery_lives'][0]==1
    with pytest.raises(ValueError,match='universe'):
        prepared_scope_coverage(universe,lives.head(3),missing,tables)


def test_scope_never_drops_unverified_products_or_infers_dates_from_product_membership():
    daily, verified, final = frames()
    rows = {r['product']: r for r in coverage_table(daily, verified, final).to_dicts()}
    assert set(rows) == {'TX', 'DN2'}
    assert rows['TX']['verified_contract_days'] == 1
    assert rows['TX']['unverified_contract_days'] == 1
    assert rows['TX']['parent_expiry_prices_needing_repair'] == 1
    assert rows['DN2']['verified_contract_days'] == 0
    assert rows['DN2']['adjusted_product_code']
    assert rows['DN2']['unmatched_parent_expiry_rows'] == 1
    assert rows['DN2']['training_admission'] == 'historical_rules_not_yet_verified'


def test_different_physical_identity_cannot_borrow_a_verified_rule():
    daily, verified, final = frames()
    verified = verified.with_columns(pl.lit('TX-201102').alias('physical_contract'))
    assert coverage_table(daily, verified, final)['verified_contract_days'].sum() == 0


def test_conflicting_settlement_and_duplicate_admission_fail_closed():
    daily, verified, final = frames()
    with pytest.raises(ValueError, match='duplicate official settlement'):
        coverage_table(daily, verified, pl.concat([final, final]))
    with pytest.raises(ValueError, match='duplicate verified'):
        coverage_table(daily, pl.concat([verified, verified]), final)


def test_expiry_review_preserves_cash_and_rights_without_inventing_a_multiplier():
    daily, _, final = frames()
    daily = daily.with_columns(pl.col('date').alias('resolved_last_trade_date'))
    final = final.with_columns(pl.lit(1620777.125).alias('final_settlement_value'))
    row = expiry_worklist(daily, final).filter(pl.col('product') == 'TX').row(0, named=True)
    assert row['contract_multiplier'] == 200.
    assert row['final_settlement_price'] == 8100.125
    assert row['final_settlement_value'] == 1620777.125
    assert row['official_value_minus_parent_price_times_multiplier'] == pytest.approx(752.125)


def test_raw_universe_keeps_codes_excluded_by_old_model_without_admitting_candidates():
    daily,verified,final=frames()
    raw=pl.DataFrame(dict(product=['TX','DN2','DN1'],product_name=['index','adjust2','adjust1'],
        asset_class=['index_future','stock_future','stock_future'],source_start=['2011-01-03']*3,
        source_end=['2011-01-04']*3,source_rows=[2,1,1],needs_dated_deliverables=[False,True,True]))
    events=pl.DataFrame(dict(product=['DN1'],issue_date_bound=[True],chronological=[True],
                             requires_reversion_review=[False]))
    x=expand_raw_universe(coverage_table(daily,verified,final),raw,events)
    r=x.filter(pl.col('product')=='DN1').row(0,named=True)
    assert x.height==3 and not r['in_original_parent']
    assert r['candidate_margin_events']==1 and r['verified_contract_days']==0
    assert r['source_contract_days'] is None
    assert r['training_admission']=='missing_from_parent_requires_physical_and_rule_preparation'


def test_currency_scope_evidence_survives_coverage_without_promoting_rules():
    daily, verified, final = frames()
    raw = pl.DataFrame(dict(product=['TX', 'DN2'], product_name=['index', 'adjusted'],
        asset_class=['index_future', 'stock_future'], source_start=['2011-01-03']*2,
        source_end=['2011-01-04']*2, source_rows=[2, 1], needs_dated_deliverables=[False, True],
        settlement_currency=['TWD']*2, currency_evidence_key=['TX', 'STF'],
        currency_source_url=['https://www.taifex.com.tw/cht/2/tX', 'https://www.taifex.com.tw/cht/2/sTF'],
        currency_source_content_sha256=['a'*64, 'b'*64]))
    out = expand_raw_universe(coverage_table(daily, verified, final), raw)
    assert set(out['settlement_currency']) == {'TWD'}
    adjusted = out.filter(pl.col('product') == 'DN2').row(0, named=True)
    assert adjusted['currency_evidence_key'] == 'STF'
    assert adjusted['currency_source_content_sha256'] == 'b'*64
    assert adjusted['verified_contract_days'] == 0
    assert adjusted['training_admission'] == 'historical_rules_not_yet_verified'
