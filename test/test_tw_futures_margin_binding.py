"""Temporal rule binding cannot manufacture executable margin history."""
from datetime import date
import hashlib
import json

import polars as pl
import pytest

from stockagent.data.tw_futures_margin_preparation import align_product_margin_intervals


def securities_transfer_case():
    text=('調整生效日：109年2月3日。加掛標準契約：契約代號AAF約定標的物2,000股標的證券。'
          '部位限制：AAF、AA1與AA2部位合併計算。AA2契約乘數不調整（仍為2,100）')
    own=[dict(product='AA1',from_product='AAF',effective_date='2020-02-03',
              contract_multiplier=2000.,deliverable_security_quantity=2000.,
              contract_months=['202006'],issue_date_bound=True),
         dict(product='AA2',from_product='AA1',effective_date='2020-02-03',
              contract_multiplier=2100.,deliverable_security_quantity=2100.,
              contract_months=['202006'],issue_date_bound=True,subscription_rights_at_final_settlement=True)]
    return text,own


@pytest.mark.parametrize('problem',[None,'source','publication','known','review','method','ratio',
    'standard_quantity','target_source','target_month','literal_group','literal_units'])
def test_securities_transfer_reuses_only_same_original_revalidated_standard_cells(problem):
    from stockagent.data.tw_futures_margin_preparation import unchanged_quantity_securities_position_groups
    text,own=securities_transfer_case()
    text=text.replace('2,000股','2.000股')
    provenance=dict(source_content_sha256='a'*64,source_url='official',published_date='2020-01-20',
        known_at='2020-01-20T23:59:59+08:00')
    for row in own:row.update(provenance)
    reviewed=dict(provenance,product='AA1',combined_position_base_product='AAF',effective_date='2020-02-03',
        effective_phase='product_regular_open',issue_date_bound=True,unit='contracts',
        event_type='combined_position_formula',combined_position_ratio='1/1',
        combined_position_evidence='AAF、AA1與AA2部位合併計算',
        extraction_method='source_bound_visual_corporate_position_group',visual_review_sha256='b'*64)
    if problem=='source':reviewed['source_content_sha256']='c'*64
    if problem=='publication':reviewed['published_date']='2020-01-19'
    if problem=='known':reviewed['known_at']='2020-01-19T23:59:59+08:00'
    if problem=='review':reviewed['visual_review_sha256']=None
    if problem=='method':reviewed['extraction_method']='unchecked_visual'
    if problem=='ratio':reviewed['combined_position_ratio']='2/1'
    if problem=='standard_quantity':own[0]['contract_multiplier']=2100.
    if problem=='target_source':own[1]['source_content_sha256']='c'*64
    if problem=='target_month':own[1]['contract_months']=[]
    if problem=='literal_group':text=text.replace('AAF、AA1與AA2','AAF、AA1與AA3')
    if problem=='literal_units':text=text.replace('2.000股','2,100股')
    assert not unchanged_quantity_securities_position_groups(text,own)
    rows=unchanged_quantity_securities_position_groups(text,own,reviewed_standard_groups=(reviewed,))
    if problem:assert not rows
    else:
        (row,)=rows;assert row['combined_position_ratio']=='21/20'
        proof=json.loads(row['position_group_unit_evidence'])
        assert proof['standard_cells_source_sha256']=='a'*64
        assert proof['standard_cells_review_sha256']=='b'*64


@pytest.mark.parametrize('problem',[None,'missing_clause','changed','punctuation','options_group',
    'other_root','wrong_origin','missing_month','missing_rights','quantity','unbound','standard_changed'])
def test_securities_transfer_parser_needs_own_literal_group_and_unchanged_units(problem):
    from stockagent.data.tw_futures_margin_preparation import unchanged_quantity_securities_position_groups
    text,own=securities_transfer_case()
    if problem=='missing_clause':text=text.split('AA2契約乘數')[0]
    if problem=='changed':text=text.replace('不調整','調整')
    if problem=='punctuation':text=text.replace('2,100','2.100')
    if problem=='options_group':text=text.replace('AAF、AA1與AA2部位','AAO、AAA與AAB部位')
    if problem=='other_root':own[1]['product']='BB2'
    if problem=='wrong_origin':own[1]['from_product']='AAF'
    if problem=='missing_month':own[1]['contract_months']=[]
    if problem=='missing_rights':own[1]['subscription_rights_at_final_settlement']=False
    if problem=='quantity':own[1]['deliverable_security_quantity']=2200.
    if problem=='unbound':own[1]['issue_date_bound']=False
    if problem=='standard_changed':own[0]['contract_multiplier']=2100.
    parsed=unchanged_quantity_securities_position_groups(text,own)
    if problem:assert not parsed
    else:
        (row,)=parsed
        assert row['event_type']=='combined_securities_position_formula'
        assert row['combined_position_ratio']=='21/20' and row['natural_person_limit'] is None
        assert row['combined_products']==['AA1','AA2','AAF']
        assert json.loads(row['position_group_unit_evidence'])['contract_months']==['202006']


@pytest.mark.parametrize('problem',[None,'contracts','wrong_base_units','missing_old_member','ended_base',
    'unpublished_base','own_source','own_month','own_quantity','reuse_same_quantity','missing_law'])
def test_securities_transfer_only_binds_existing_share_pool_and_own_physical_month(problem):
    from stockagent.data.tw_futures_margin_preparation import (unchanged_quantity_securities_position_groups,
        position_candidate_intervals,bind_equity_position_families,bind_physical_position_inputs)
    text,own=securities_transfer_case();(formula,)=unchanged_quantity_securities_position_groups(text,own)
    d=date(2020,2,4);prod='AA2';month='202006'
    proof=dict(issue_date_bound=True,source_content_sha256='a'*64,source_url='old',
        effective_date='2020-01-01',effective_phase='product_regular_open',
        known_at='2019-12-20T23:59:59+08:00')
    base=dict(proof,product='AAF',event_type='corporate_securities_unit_limit',unit='shares',
        natural_person_limit=4_000_000.,position_unit=2000.,combined_products=['AA1','AAF'])
    if problem=='contracts':base.update(event_type='absolute_level',unit='contracts',natural_person_limit=2000.)
    if problem=='wrong_base_units':base['position_unit']=2100.
    if problem=='missing_old_member':base['combined_products']=['AAF','AA3']
    if problem=='ended_base':base['valid_until_date_inclusive']='2020-02-02'
    if problem=='unpublished_base':base['known_at']='2020-02-04T23:59:59+08:00'
    formula.update(proof,source_content_sha256='b'*64,source_url='new',effective_date='2020-02-03',
        known_at='2020-01-20T23:59:59+08:00')
    levels,issues=position_candidate_intervals([base,formula]);assert not issues
    ints=pl.DataFrame(levels,infer_schema_length=None,schema_overrides={
        'admission_not_before':pl.String,'valid_until_date_exclusive':pl.String,'monthly_position_limit':pl.Float64})
    u=pl.DataFrame(dict(product=['AAF','AA1','AA2','AA3'],product_name=['甲期貨']*4,
        underlying_symbol=['1000']*4,asset_class=['stock_future']*4))
    ds=pl.DataFrame(dict(date=[d],product=[prod],contract=[month]))
    unit_source='b'*64 if problem not in ('own_source','reuse_same_quantity') else 'c'*64
    units=ds.with_columns(pl.lit('bound_prior_publication').alias('terms_binding_status'),
        pl.lit(2200. if problem=='own_quantity' else 2100.).alias('contract_multiplier'),
        pl.lit('2020-01-20T23:59:59+08:00').alias('known_at'),
        pl.lit([unit_source]).alias('source_content_sha256s'))
    declarations=pl.DataFrame(dict(product=[prod],contract=['202003' if problem=='own_month' else month],
        source_content_sha256s=[['b'*64]]))
    law=dict(effective_date='2010-01-01',known_at='2009-12-01T23:59:59+08:00',standard_units=2000.,
        mini_units=None,asset_class='stock_future',source_content_sha256='f'*64)
    laws=[] if problem=='missing_law' else [law]
    pos=bind_equity_position_families(ds,ints,u,units,laws)
    bound=bind_physical_position_inputs(ds,pos,units,u,laws,corporate_unit_intervals=declarations)
    assert bound['position_numeric_inputs_resolved'].to_list()==[problem is None]
    assert bound['position_limit'].to_list()==([4_000_000.] if problem is None else [None])
    if problem is None:
        assert bound['position_unit'][0]==2100.
        assert set(bound['source_content_sha256s'][0])=={'a'*64,'b'*64}
        assert bound['position_base_interval_id'][0] is not None



def test_same_code_dividend_does_not_require_an_identity_transfer():
    from stockagent.data.tw_futures_margin_preparation import corporate_identity_boundaries
    facts = pl.DataFrame([dict(product='CCF', from_product='CCF',
        effective_date='2026-07-02', contract_months=['202607'],
        equity_credit_long_per_contract=200., source_content_sha256='a'*64)])
    boundaries, edges = corporate_identity_boundaries(facts)
    assert boundaries.is_empty() and edges.is_empty()
    assert boundaries.schema['corporate_boundary'] == pl.Date
    assert edges.schema['corporate_boundary_sources'] == pl.List(pl.String)


def test_declared_transfer_with_missing_months_is_not_a_normal_only_scope():
    from stockagent.data.tw_futures_margin_preparation import corporate_identity_boundaries
    facts = pl.DataFrame([dict(product='CC1', from_product='CCF',
        effective_date='2026-07-02', source_content_sha256='a'*64)])
    with pytest.raises(ValueError, match='no source-bound corporate identity transfers'):
        corporate_identity_boundaries(facts)


@pytest.mark.parametrize('problem', [None, 'source', 'incomplete', 'table_sha', 'receipt_sha'])
def test_retained_margin_grids_require_exact_original_and_complete_receipt(tmp_path, problem):
    from types import SimpleNamespace
    from downloader.artifact_io import sha256_file
    from scripts.build_tw_futures_margin_event_candidates import retained_document_table_views
    table = tmp_path / 'tables.json'
    table.write_text(json.dumps(dict(pages=[dict(page=1, native_text='original', tables=[])])))
    proof = dict(status='complete', content_sha256='a'*64,
                 files=[dict(path='tables.json', sha256=sha256_file(table))])
    if problem == 'source': proof['content_sha256'] = 'b'*64
    if problem == 'incomplete': proof['status'] = 'partial'
    if problem == 'table_sha': proof['files'][0]['sha256'] = 'c'*64
    receipt = tmp_path / 'receipt.json'
    receipt.write_text(json.dumps(proof))
    sources = [dict(kind='native_table_tables.json', path=table.name, sha256=sha256_file(table)),
               dict(kind='native_table_receipt.json', path=receipt.name, sha256=sha256_file(receipt))]
    if problem == 'receipt_sha': sources[1]['sha256'] = 'd'*64
    archive = SimpleNamespace(bundle=tmp_path)
    def read():
        return list(retained_document_table_views(archive, 'official', dict(content_sha256='a'*64), sources))
    if problem:
        with pytest.raises(ValueError): read()
    else:
        (pages, extraction), = read()
        assert pages[0]['native_text'] == 'original'
        assert extraction == 'native_table_tables.json:' + sha256_file(table)


def test_disposal_closure_postponement_requires_complete_prints_and_explicit_rule():
    from stockagent.data.tw_futures_margin_preparation import disposal_margin_restorations
    fact=dict(product='AAF',margin_kind='notional_rate',after=[.3,.23,.22],before=[.2,.15,.14],
        effective_date='2023-07-26',published_date='2023-07-25',issue_date_bound=True,
        known_at='2023-07-25T23:59:59+08:00',source_content_sha256='a'*64,
        requires_reversion_review=True,restoration_rule='return_to_declared_before',
        restoration_delay_rule='postpone_for_closed_cash_sessions',temporary_end_evidence=json.dumps([
            dict(date_iso='2023-08-09',boundary='after_regular_session')]))
    u=pl.DataFrame(dict(product=['AAF'],underlying_symbol=['1000']))
    disposition=pl.DataFrame([dict(date='2023-07-24',stock_id='1000',period_start='2023-07-25',
        period_end='2023-08-09',measure='處置期間（十二個營業日）',source_sha256='b'*64)])
    days=['2023-07-25','2023-07-26','2023-07-27','2023-07-28','2023-07-31',
          '2023-08-01','2023-08-02','2023-08-04','2023-08-07','2023-08-08','2023-08-09','2023-08-10']
    obs=pl.DataFrame(dict(date=days,symbol=['1000']*12,volume=[1.]*12,source_sha256=['c'*64]*12))
    closure=dict(date='2023-08-03',known_at='2023-08-02T23:59:59+08:00',
        source_content_sha256='d'*64,stock_ids=['1000'],disposition_source_sha256s=['b'*64])
    def resolve(facts=(fact,),observations=obs,closures=(closure,),dispositions=disposition):
        return disposal_margin_restorations(list(facts),u,dispositions,observations,market_closures=closures)
    rows,issues=resolve()
    assert not issues and len(rows)==1
    restored=rows[0]
    assert restored['effective_date']=='2023-08-10' and restored['after']==fact['before']
    assert restored['effective_phase']=='after_product_regular_close'
    proof=json.loads(restored['restoration_evidence'])
    assert proof['completed_cash_dates']==days and proof['required_sessions']==12
    assert proof['delayed_for_official_closure']['nominal_end']=='2023-08-09'
    # Neither missing observations nor an unrelated closure prove postponement.
    assert not resolve(closures=())[0]
    assert not resolve(closures=[dict(closure,stock_ids=['2000'])])[0]
    assert not resolve(closures=[dict(closure,disposition_source_sha256s=['e'*64])])[0]
    assert not resolve(observations=obs.filter(pl.col('date')!='2023-08-02'))[0]
    assert not resolve(facts=[dict(fact,restoration_delay_rule=None)])[0]
    # A later extension or an intervening amount still needs composed rules.
    extended=pl.concat([disposition,disposition.with_columns(pl.lit('2023-08-11').alias('period_end'))])
    assert not resolve(dispositions=extended)[0]
    assert not resolve(facts=[fact,dict(fact,effective_date='2023-08-10',requires_reversion_review=False)])[0]
    with pytest.raises(ValueError,match='prior-publication'):
        resolve(closures=[dict(closure,known_at='2023-08-03T23:59:59+08:00')])


@pytest.mark.parametrize('problem', [None, 'earlier_event', 'opening_phase', 'late_publication',
    'different_before', 'missing_source', 'conflicting_successors', 'missing_cash_session'])
def test_observed_restoration_and_exact_same_boundary_grade_form_one_chain(problem):
    from stockagent.data.tw_futures_margin_preparation import (
        disposal_margin_restorations, margin_candidate_intervals)
    fact = dict(product='AAF', margin_kind='notional_rate', after=[.3, .23, .22],
        before=[.2, .15, .14], effective_date='2023-07-26', published_date='2023-07-25',
        effective_phase='after_product_regular_close', issue_date_bound=True,
        known_at='2023-07-25T23:59:59+08:00', source_content_sha256='a'*64,
        source_url='https://www.taifex.com.tw/own-disposal.pdf',
        requires_reversion_review=True, restoration_rule='return_to_declared_before',
        restoration_delay_rule='postpone_for_closed_cash_sessions',
        temporary_end_evidence=json.dumps([dict(date_iso='2023-08-09', boundary='after_regular_session')]))
    successor = dict(product='AAF', margin_kind='notional_rate', before=fact['before'],
        after=[.25, .19, .18], effective_date='2023-08-10', published_date='2023-08-09',
        effective_phase='after_product_regular_close', issue_date_bound=True,
        known_at='2023-08-09T23:59:59+08:00', source_content_sha256='e'*64,
        source_url='https://www.taifex.com.tw/own-grade.pdf',
        requires_reversion_review=False, temporary_end_evidence='[]')
    days = ['2023-07-25', '2023-07-26', '2023-07-27', '2023-07-28', '2023-07-31',
        '2023-08-01', '2023-08-02', '2023-08-04', '2023-08-07', '2023-08-08',
        '2023-08-09', '2023-08-10']
    if problem == 'earlier_event': successor['effective_date'] = '2023-08-09'
    if problem == 'opening_phase': successor['effective_phase'] = 'product_regular_open'
    if problem == 'late_publication': successor['known_at'] = '2023-08-10T23:59:59+08:00'
    if problem == 'different_before': successor['before'] = [.24, .18, .17]
    if problem == 'missing_source': successor['source_content_sha256'] = None
    successors = [successor]
    if problem == 'conflicting_successors':
        successors.append(dict(successor, after=[.26, .2, .19], source_content_sha256='f'*64))
    observations = pl.DataFrame(dict(date=days, symbol=['1000']*12,
        volume=[1.]*12, source_sha256=['c'*64]*12))
    if problem == 'missing_cash_session': observations = observations.filter(pl.col('date') != '2023-08-02')
    disposition = pl.DataFrame([dict(date='2023-07-24', stock_id='1000',
        period_start='2023-07-25', period_end='2023-08-09',
        measure='處置期間（十二個營業日）', source_sha256='b'*64)])
    closure = dict(date='2023-08-03', known_at='2023-08-02T23:59:59+08:00',
        source_content_sha256='d'*64, stock_ids=['1000'], disposition_source_sha256s=['b'*64])
    restored, issues = disposal_margin_restorations([fact, *successors],
        pl.DataFrame(dict(product=['AAF'], underlying_symbol=['1000'])),
        disposition, observations, market_closures=[closure])
    if problem is not None:
        assert not restored and issues
        return
    assert len(restored) == 1 and not issues
    assert restored[0]['after'] == fact['before'] and restored[0]['before'] == fact['after']
    evidence = json.loads(restored[0]['restoration_evidence'])
    assert evidence['same_boundary_margin_successor']['source_content_sha256s'] == ['e'*64]
    # The canonical restoration builder retains the literal nominal boundary
    # separately and binds the proved actual boundary on its derived fact.
    extended = dict(fact, original_temporary_end_evidence=fact['temporary_end_evidence'],
        temporary_end_evidence=json.dumps([dict(date_iso='2023-08-10', boundary='after_regular_session')]))
    levels, errors = margin_candidate_intervals([extended, *restored, successor])
    assert not errors and len(levels) == 2
    assert levels[0]['initial'] == .3 and levels[0]['valid_until_date_exclusive'] == '2023-08-10'
    assert levels[1]['initial'] == .25 and levels[1]['maintenance'] == .19
    assert levels[1]['known_at'] == '2023-08-10T13:35:00+08:00'
    assert levels[1]['source_content_sha256s'] == ['a'*64, 'e'*64]
    # A later numeric/source change cannot inherit a previously pinned chain.
    changed = dict(successor, after=[.26, .2, .19])
    _, rejected = margin_candidate_intervals([extended, *restored, changed])
    assert any(r['reasons'] == 'conflicting_or_unresolved_margin_boundary' for r in rejected)


@pytest.mark.parametrize('problem',[None,'missing_halt','closure_only_rule','wrong_security',
    'wrong_disposition','missing_positive_session','contradictory_positive_halt','late_halt_notice',
    'late_resumption_notice','partial_halt','partial_resumption','missing_resumption_source'])
def test_restoration_uses_a_separate_official_full_day_security_halt_episode(problem):
    from stockagent.data.tw_futures_margin_preparation import disposal_margin_restorations
    fact=dict(product='LXF',margin_kind='notional_rate',after=[.2025,.1553,.15],before=[.135,.1035,.10],
        effective_date='2018-05-16',published_date='2018-05-15',issue_date_bound=True,
        known_at='2018-05-15T23:59:59+08:00',source_content_sha256='a'*64,
        requires_reversion_review=True,restoration_rule='return_to_declared_before',
        restoration_delay_rule='postpone_for_closed_or_full_day_halted_cash_sessions',
        temporary_end_evidence=json.dumps([dict(date_iso='2018-05-28',boundary='after_regular_session')]))
    universe=pl.DataFrame(dict(product=['LXF'],underlying_symbol=['2327']))
    dispo=pl.DataFrame([dict(date='2018-05-14',stock_id='2327',period_start='2018-05-15',
        period_end='2018-05-28',measure='處置期間（十個營業日）',source_sha256='b'*64)])
    days=['2018-05-15','2018-05-16','2018-05-17','2018-05-18','2018-05-21',
        '2018-05-23','2018-05-24','2018-05-25','2018-05-28','2018-05-29']
    obs=pl.DataFrame(dict(date=days+['2018-05-22'],symbol=['2327']*11,
        volume=[1.]*10+[0.],source_sha256=['c'*64]*11))
    halt=dict(date='2018-05-22',stock_id='2327',suspension_time='08:00:00',
        known_at='2018-05-21T23:59:59+08:00',source_content_sha256='d'*64,
        resumption_date='2018-05-23',resumption_time='08:00:00',
        resumption_known_at='2018-05-22T23:59:59+08:00',resumption_source_content_sha256='e'*64,
        historical_observation_source_sha256='f'*64,disposition_source_sha256s=['b'*64])
    if problem=='closure_only_rule':fact['restoration_delay_rule']='postpone_for_closed_cash_sessions'
    if problem=='wrong_security':halt['stock_id']='2330'
    if problem=='wrong_disposition':halt['disposition_source_sha256s']=['1'*64]
    if problem=='missing_positive_session':obs=obs.filter(pl.col('date')!='2018-05-17')
    if problem=='contradictory_positive_halt':obs=obs.with_columns(pl.lit(1.).alias('volume'))
    if problem=='late_halt_notice':halt['known_at']='2018-05-22T10:00:00+08:00'
    if problem=='late_resumption_notice':halt['resumption_known_at']='2018-05-23T10:00:00+08:00'
    if problem=='partial_halt':halt['suspension_time']='10:00:00'
    if problem=='partial_resumption':halt['resumption_time']='10:00:00'
    if problem=='missing_resumption_source':halt.pop('resumption_source_content_sha256')
    def resolve():
        return disposal_margin_restorations([fact],universe,dispo,obs,
            security_full_day_halts=[] if problem=='missing_halt' else [halt])
    if problem in ('late_halt_notice','late_resumption_notice','partial_halt','partial_resumption','missing_resumption_source'):
        with pytest.raises(ValueError,match='security halt requires'):resolve()
    elif problem:
        rows,issues=resolve()
        assert not rows and issues
    else:
        rows,issues=resolve()
        assert not issues and len(rows)==1
        assert rows[0]['effective_date']=='2018-05-29' and rows[0]['after']==fact['before']
        proof=json.loads(rows[0]['restoration_evidence'])
        assert proof['completed_cash_dates']==days and proof['required_sessions']==10
        assert proof['delayed_for_official_closure'] is None
        delayed=proof['delayed_for_official_security_halt']
        assert delayed['official_closures']==[] and delayed['official_security_halts']==[halt]
        assert delayed['nominal_end']=='2018-05-28' and delayed['actual_end']=='2018-05-29'


def test_revoked_notice_ends_old_caps_and_cannot_activate_a_future_exception():
    from stockagent.data.tw_futures_margin_preparation import position_candidate_intervals
    proof=dict(issue_date_bound=True,source_content_sha256='a'*64,source_url='official',
        known_at='2013-01-15T23:59:59+08:00',effective_date='2013-01-15',
        effective_phase='product_regular_open',unit='contracts',natural_person_limit=350,
        notice_revoked_effective_date='2013-02-20',notice_revocation_source_sha256='b'*64,
        notice_revocation_review_sha256='c'*64)
    rows,issues=position_candidate_intervals([dict(proof,product='AAF'),
        dict(proof,product='BBF',effective_date='2013-09-19')])
    assert len(rows)==1 and rows[0]['valid_until_date_exclusive']=='2013-02-20'
    assert any(r['reasons']=='notice_revoked_before_scheduled_effective_date' for r in issues)
    with pytest.raises(ValueError,match='revocation lacks'):
        position_candidate_intervals([dict(proof,product='AAF',notice_revocation_source_sha256=None)])


def test_position_units_are_independent_of_cash_but_keep_conflict_and_transfer_barriers():
    from stockagent.data.tw_futures_margin_preparation import corporate_terms_intervals,bind_dated_corporate_terms
    fact=dict(product='AA1',from_product='AAF',effective_date='2020-01-01',
        contract_months=['202006'],contract_multiplier=2100.,deliverable_security_quantity=2100.,issue_date_bound=True,
        source_content_sha256='a'*64,source_url='official',known_at='2019-12-20T23:59:59+08:00',
        deliverable_components_resolved=False,has_equity_credit_fields=True,cash_equity_pair_agrees=False)
    full,_=corporate_terms_intervals([fact])
    assert full==[]
    units,_=corporate_terms_intervals([fact],unit_only=True)
    assert len(units)==1 and 'deliverable_cash_twd' not in units[0]
    levels=pl.DataFrame(units,schema_overrides={'valid_until_exclusive':pl.String})
    days=pl.DataFrame(dict(date=[date(2020,1,2)],product=['AA1'],contract=['202006']))
    bound=bind_dated_corporate_terms(days,levels,unit_only=True)
    assert bound['contract_multiplier'][0]==2100.
    with pytest.raises(ValueError,match='incomplete dated corporate term'):
        bind_dated_corporate_terms(days,levels)
    # A truncated OCR multiplier cannot overrule a matching quantity/multiplier
    # pair in another view of the same source, or become a standalone unit.
    truncated=dict(fact,contract_multiplier=2.)
    assert not corporate_terms_intervals([truncated],unit_only=True)[0]
    assert len(corporate_terms_intervals([fact,truncated],unit_only=True)[0])==1
    conflicting=dict(fact,contract_multiplier=2000.,deliverable_security_quantity=2000.)
    assert not corporate_terms_intervals([fact,conflicting],unit_only=True)[0]
    outgoing=dict(fact,from_product='AA1',product='AA2',effective_date='2020-02-01',
        contract_multiplier=None,source_content_sha256='b'*64)
    ended,_=corporate_terms_intervals([fact,outgoing],unit_only=True)
    assert len(ended)==1 and ended[0]['valid_until_exclusive']=='2020-02-01'


@pytest.mark.parametrize('unit',[2100.,2200.])
def test_position_review_does_not_require_unrelated_subscription_value(tmp_path,unit):
    from types import SimpleNamespace
    from scripts.build_tw_futures_margin_event_candidates import position_source_review_candidates
    from downloader.artifact_io import sha256_file
    image=tmp_path/'page.png';image.write_bytes(b'synthetic reviewed page')
    page=dict(page=2,path=image.name,sha256=sha256_file(image),native_text='AAF與AA1部位合併計算。',tables=[
        dict(cells=[['持有部位','AAF','AA1'],['每口折算股數','2,000','2,100']]),
        dict(cells=[['適用期間','自99.01.04起至99.03.17止'],['自然人','2,100,000股']])])
    path=tmp_path/'review.json';path.write_text(json.dumps(dict(reviews=[dict(
        review_kind='source_bound_visual_corporate_position_cells',source_url='official',
        content_sha256='a'*64,published_date='2010-01-01',effective_date='2010-01-04',pages=[page])])))
    class Archive:
        conn=SimpleNamespace(execute=lambda *args:SimpleNamespace(fetchone=lambda:{'published_date':'2010-01-01'}))
        def document(self,url):return {'content_sha256':'a'*64}
        def copy(self,p,h,**kwargs):assert sha256_file(p)==h
    fact=dict(product='AA1',from_product='AAF',effective_date='2010-01-04',
        source_content_sha256='a'*64,source_url='official',issue_date_bound=True,
        known_at='2010-01-01T23:59:59+08:00',contract_multiplier=unit,
        deliverable_components_resolved=False,requires_rights_valuation=True)
    if unit!=2100:
        with pytest.raises(ValueError,match='disagree'):
            position_source_review_candidates(Archive(),path,[fact])
    else:
        result=position_source_review_candidates(Archive(),path,[fact])
        assert {r['product'] for r in result}=={'AAF','AA1'}
        assert {r['natural_person_limit'] for r in result}=={2100000}


@pytest.mark.parametrize('problem', ['valid', 'changed_unit', 'unknown_member', 'late_clock',
                                    'retained_page','changed_retained_page','wrong_retained_source',
                                    'retained_corporate_page','wrong_retained_corporate_source'])
def test_reviewed_position_group_only_binds_source_owned_unchanged_units(tmp_path, problem):
    from types import SimpleNamespace
    from scripts.build_tw_futures_margin_event_candidates import position_source_review_candidates
    from downloader.artifact_io import sha256_file
    image = tmp_path/'page.png'; image.write_bytes(b'synthetic reviewed group page')
    text = ('加掛標準契約：契約代號AAF約定標的物2,000股標的證券。'
            '部位限制：AAF、AA1與AA2部位合併計算。')
    if problem == 'unknown_member': text = text.replace('與AA2', '與AA3')
    review = dict(review_kind='source_bound_visual_corporate_position_group', source_url='official',
        content_sha256='a'*64, published_date='2014-09-29', effective_date='2014-10-07',
        pages=[dict(page=4, path=image.name, sha256=sha256_file(image), native_text=text)])
    sources={}
    if 'retained' in problem:
        review['pages'][0]['path']='missing_original_location.png'
        sources['page']=dict(path=image.name,sha256=sha256_file(image),
            kind='visual_corporate_review_page' if 'corporate' in problem else 'visual_position_review_page',
            url='foreign' if problem.startswith('wrong_retained') else 'official')
    path = tmp_path/'review.json'; path.write_text(json.dumps(dict(reviews=[review])))
    if problem=='changed_retained_page':image.write_bytes(b'changed copied image')
    class Archive:
        bundle=tmp_path
        conn=SimpleNamespace(execute=lambda *args:SimpleNamespace(fetchone=lambda:{'published_date':'2014-09-29'}))
        def document(self, url): return {'content_sha256':'a'*64}
        def __init__(self):self.sources=sources
        def copy(self, p, h, **kwargs):
            if sha256_file(p)!=h:raise ValueError('archive source hash mismatch')
    common = dict(effective_date='2014-10-07', source_url='official',
        source_content_sha256='a'*64, known_at='2014-09-29T23:59:59+08:00', issue_date_bound=True)
    facts = [dict(common, from_product='AAF', product='AA1', contract_multiplier=2000.),
             dict(common, from_product='AA1', product='AA2', contract_multiplier=2161.6)]
    if problem == 'changed_unit': facts[0]['contract_multiplier'] = 2100.
    if problem == 'late_clock': facts[0]['known_at'] = '2014-10-07T23:59:59+08:00'
    if problem in {'valid','retained_page','retained_corporate_page'}:
        row, = position_source_review_candidates(Archive(), path, facts)
        assert row['product'] == 'AA1' and row['combined_position_ratio'] == '1/1'
        assert row['natural_person_limit'] is None
        assert row['extraction_method'] == review['review_kind']
        from stockagent.data.tw_futures_margin_preparation import position_candidate_intervals
        intervals, issues = position_candidate_intervals([row])
        assert not issues and len(intervals) == 1
        assert intervals[0]['requires_base_limit_join']
        for change in [dict(visual_review_sha256=None), dict(position_formula_parser=None)]:
            rejected, errors = position_candidate_intervals([dict(row, **change)])
            assert not rejected and any('conversion_invalid' in e['reasons'] for e in errors)
    else:
        with pytest.raises(ValueError, match='disagree|lacks dated|source hash mismatch|page identity'):
            position_source_review_candidates(Archive(), path, facts)


@pytest.mark.parametrize('problem', ['truncated_view', 'standalone_truncation', 'real_conflict',
                                    'conflict_in_other_month'])
def test_position_review_reuses_resolved_units_without_hiding_real_conflicts(tmp_path, problem):
    from types import SimpleNamespace
    from downloader.artifact_io import sha256_file
    from scripts.build_tw_futures_margin_event_candidates import position_source_review_candidates
    image=tmp_path/'page.png'; image.write_bytes(b'inspected position page')
    page=dict(page=2,path=image.name,sha256=sha256_file(image),native_text='AAF與AA1部位合併計算。',tables=[
        dict(cells=[['持有部位','AAF','AA1'],['每口折算股數','2,000','2,100']]),
        dict(cells=[['適用期間','自99.01.04起至99.03.17止'],['自然人','2,100,000股']])])
    path=tmp_path/'review.json'; path.write_text(json.dumps(dict(reviews=[dict(
        review_kind='source_bound_visual_corporate_position_cells',source_url='official',
        content_sha256='a'*64,published_date='2010-01-01',effective_date='2010-01-04',pages=[page])])))
    class Archive:
        conn=SimpleNamespace(execute=lambda *args:SimpleNamespace(fetchone=lambda:{'published_date':'2010-01-01'}))
        def document(self,url): return {'content_sha256':'a'*64}
        def copy(self,p,h,**kwargs): assert sha256_file(p)==h
    fact=dict(product='AA1',from_product='AAF',effective_date='2010-01-04',
        source_content_sha256='a'*64,source_url='official',issue_date_bound=True,
        known_at='2010-01-01T23:59:59+08:00',contract_multiplier=2100.,
        deliverable_security_quantity=2100.,contract_months=['201003','201006'],
        deliverable_components_resolved=False,requires_rights_valuation=True)
    damaged=dict(fact,contract_multiplier=210.)
    facts=[fact,damaged]
    if problem=='standalone_truncation': facts=[damaged]
    if problem=='real_conflict': damaged['deliverable_security_quantity']=210.
    if problem=='conflict_in_other_month':
        damaged.update(deliverable_security_quantity=210.,contract_months=['201006'])
    before=json.dumps(facts,sort_keys=True)
    if problem=='truncated_view':
        rows=position_source_review_candidates(Archive(),path,facts)
        assert {r['product'] for r in rows}=={'AAF','AA1'}
        assert {r['natural_person_limit'] for r in rows}=={2100000}
        assert all('deliverable_cash_twd' not in r for r in rows)
    else:
        with pytest.raises(ValueError,match='disagree'):
            position_source_review_candidates(Archive(),path,facts)
    assert json.dumps(facts,sort_keys=True)==before


def test_dated_full_position_roster_never_confuses_grades_options_or_decimal_ocr():
    from stockagent.data.tw_futures_margin_preparation import position_grid_candidates
    rows=[['商品代號(前2碼)','級數','證券代號','股票期貨','股票選擇權','自然人'],
          ['','','','','',''],['','','','','',''],
          ['AA','3','1000','O','O','2,000'],['BB','1','2000','','O','8,000'],
          ['CC','2','3000','O','','4.000'],['DD','3','4000','O','','']]
    source='主旨：公告放寬股票期貨交易人部位限制，並自102年2月20日起實施。單位：契約數'
    row,=position_grid_candidates(rows,source)
    assert row['product']=='AAF' and row['natural_person_limit']==2000
    assert row['effective_date']=='2013-02-20'
    assert row['effective_phase']=='date_only_requires_phase_review'
    assert not position_grid_candidates(rows,source.replace('並自102年2月20日起實施','即日起實施'))


def test_reviewed_roster_keeps_publication_clock_and_named_listing_exceptions(tmp_path):
    from types import SimpleNamespace
    from scripts.build_tw_futures_margin_event_candidates import position_source_review_candidates
    from downloader.artifact_io import sha256_file
    page=tmp_path/'page.png';page.write_bytes(b'synthetic reviewed roster')
    clause='有關測試期貨之交易人部位限制數，將俟該股票期貨上市後適用(即102年2月21日)。'
    review=dict(review_kind='source_bound_visual_position_roster_cells',source_url='official',
        content_sha256='a'*64,published_date='2013-02-20',effective_date='2013-02-20',
        transcribed_text='發文日期：中華民國102年2月20日。主旨：公告放寬股票期貨交易人部位限制，並自102年2月20日起實施。'+clause,
        pages=[dict(page=1,path=page.name,sha256=sha256_file(page))],
        rows=[dict(product='AAF',natural_person_limit=2000,page=1,futures_eligible=True),
              dict(product='BBF',product_name='測試期貨',underlying_symbol='1000',natural_person_limit=4000,page=1,
                   futures_eligible=True,effective_date='2013-02-21',effective_date_clause=clause)])
    path=tmp_path/'review.json'
    class Archive:
        conn=SimpleNamespace(execute=lambda sql,args:SimpleNamespace(fetchone=lambda:{
            'published_date':'2013-02-08' if args[0]=='old' else '2013-02-20'}))
        def document(self,url):return {'content_sha256':('b' if url=='old' else 'a')*64}
        def copy(self,p,h,**kwargs):assert sha256_file(p)==h
    def parse(facts=None):
        path.write_text(json.dumps(dict(reviews=[review])))
        return position_source_review_candidates(Archive(),path,position_facts=facts)
    old,new=parse()
    assert old['effective_phase']=='date_only_requires_phase_review'
    assert new['effective_phase']=='new_contract_listing' and new['effective_date']=='2013-02-21'
    assert old['known_at']==new['known_at']=='2013-02-20T23:59:59+08:00'
    assert all(r['candidate_only'] and not r['point_in_time_verified'] for r in (old,new))
    review['rows'][0]['futures_eligible']=False
    with pytest.raises(ValueError,match='explicit futures indicator'):parse()
    review['rows'][0]['futures_eligible']=True
    review['rows'][1]['product_name']='其他期貨'
    with pytest.raises(ValueError,match='named clause'):parse()
    review['rows'][1]['product_name']='測試期貨'
    review['rows'][1]['effective_date']='2013-02-19'
    with pytest.raises(ValueError,match='named clause'):parse()
    review['rows'][1]['effective_date']='2013-02-21'
    review['rows'][1]['supersedes_listing_caps']=[dict(source_url='old',content_sha256='b'*64,
        published_date='2013-02-08',underlying_symbol='1000',natural_person_limit=350,
        page_image=page.name,page_sha256=sha256_file(page))]
    with pytest.raises(ValueError,match='retained listing cap'):parse()
    prior=dict(product='BBF',underlying_symbol='1000',source_content_sha256='b'*64,source_url='old',
        effective_date='2013-02-21',effective_phase='product_regular_open',natural_person_limit=350,
        known_at='2013-02-08T23:59:59+08:00',unit='contracts',issue_date_bound=True)
    from stockagent.data.tw_futures_margin_preparation import position_candidate_intervals
    _,new=parse([prior])
    levels,issues=position_candidate_intervals([prior,new])
    assert not issues and len(levels)==1 and levels[0]['position_limit']==4000
    assert levels[0]['superseded_source_sha256s']==['b'*64]
    with pytest.raises(ValueError,match='retained listing cap'):parse([dict(prior,natural_person_limit=1250)])
    with pytest.raises(ValueError,match='retained listing cap'):parse([dict(prior,underlying_symbol='2000')])
    assert parse([dict(prior,underlying_symbol=None)])[1]['supersedes_source_sha256s']==['b'*64]


def test_reviewed_position_grid_keeps_raise_and_lower_dates_separate(tmp_path):
    from types import SimpleNamespace
    from scripts.build_tw_futures_margin_event_candidates import position_source_review_candidates
    from downloader.artifact_io import sha256_file
    image=tmp_path/'page.png';image.write_bytes(b'synthetic reviewed grid')
    header=['契約名稱','契約代碼','自然人']
    review=dict(review_kind='source_bound_visual_position_grid_cells',source_url='official',
        content_sha256='a'*64,published_date='2014-01-17',expected_products=['AAF','BBF'],
        transcribed_text='發文日期：中華民國103年1月17日。調整本公司股票期貨部位限制數。'
        '部位限制數為調高者，自公告日起生效；為調降者，自公告日已上市之次近月份契約到期後生效(即103年3月20日)。',
        pages=[dict(page=2,path=image.name,sha256=sha256_file(image),tables=[
            dict(caption='提高部位限制 單位：契約數',cells=[header,['測試期貨','AA','8000']]),
            dict(caption='降低部位限制 單位：契約數',cells=[header,['測試二期貨','BB','4000']])])])
    class Archive:
        conn=SimpleNamespace(execute=lambda *args:SimpleNamespace(fetchone=lambda:{'published_date':'2014-01-17'}))
        def document(self,url):return {'content_sha256':'a'*64}
        def copy(self,p,h,**kwargs):assert sha256_file(p)==h
    path=tmp_path/'review.json'
    def parse():
        path.write_text(json.dumps(dict(reviews=[review])))
        return position_source_review_candidates(Archive(),path)
    a,b=parse()
    assert a['effective_date']=='2014-01-17' and b['effective_date']=='2014-03-20'
    assert a['known_at']==b['known_at']=='2014-01-17T23:59:59+08:00'
    review['expected_products'].append('CCF')
    with pytest.raises(ValueError,match='exact products'):parse()


def test_corporate_position_merged_cap_group_and_all_contracts_delisting_scope():
    from stockagent.data.tw_futures_margin_preparation import corporate_position_table_candidates
    pages=[dict(page=3,native_text='',tables=[
        dict(cells=[['持有部位','AAF','AA1','AA2'],['每口折算股數','2,000','2,000','2,100'],
                    ['自然人','4,200,000股',None,None]]),
        dict(cells=[['適用期間','自100.08.12起至100.09.21止',
                     '自100.09.22起至AA1及AA2契約皆終止掛牌前一營業日止'],
                    ['自然人','4,200,000股','4,000,000股']])])]
    identities=[dict(product='AA1',from_product='AAF',contract_multiplier=2000.),
                dict(product='AA2',from_product='AA1',contract_multiplier=2100.)]
    rows=corporate_position_table_candidates(pages,corporate=identities)
    assert len(rows)==6 and {r['product'] for r in rows}=={'AAF','AA1','AA2'}
    end=[r for r in rows if r['effective_date']=='2011-09-22']
    assert all(r['end_rule']=='previous_business_day_before_all_named_contracts_delisting' for r in end)
    assert all(r['requires_delisting_clock'] for r in end)


def test_fractional_share_cap_is_preserved_without_fractional_contracts():
    from stockagent.data.tw_futures_margin_preparation import corporate_position_table_candidates
    pages=[dict(page=2,native_text='DXF與DX1部位合併計算',tables=[
        dict(cells=[['持有部位','DXF','DX1'],['每口折算股數','2,000','2,100.6921']]),
        dict(cells=[['適用期間','自99.07.29起至99.09.15止'],
                    ['自然人','5,251,730.25股']])])]
    identities=[dict(product='DX1',from_product='DXF',contract_multiplier=2100.6921)]
    rows=corporate_position_table_candidates(pages,corporate=identities)
    assert len(rows)==2
    assert all(r['natural_person_limit']==5251730.25 and r['unit']=='shares' for r in rows)
    adjusted=next(r for r in rows if r['product']=='DX1')
    from decimal import Decimal
    assert Decimal(str(adjusted['natural_person_limit']))/Decimal(str(adjusted['position_unit']))==2500


def test_same_security_final_fixing_retains_its_derived_identity_and_never_borrows_daily_prices():
    from stockagent.data.tw_futures_margin_preparation import derive_adjusted_final_fixings,physical_lifetime_calendar
    u=pl.DataFrame(dict(product=['AAF','AA1','BBF'],product_name=['甲期貨','甲期貨','乙期貨'],
        asset_class=['stock_future']*3,underlying_symbol=['1000','1000','2000']))
    final=pl.DataFrame([dict(product='AAF',contract='201109',settlement_date=date(2011,9,21),
        final_settlement_price=31.1,final_settlement_value=None,source_sha256='a'*64,
        settlement_method='cash_settlement',reported_date_role='final_settlement_day',source_kind='official'),
        dict(product='BBF',contract='201109',settlement_date=date(2011,9,21),
        final_settlement_price=100.,final_settlement_value=None,source_sha256='b'*64,
        settlement_method='cash_settlement',reported_date_role='final_settlement_day',source_kind='official')],
        schema_overrides={'final_settlement_value':pl.Float64})
    terms=pl.DataFrame([dict(product='AA1',contract='201109',effective_date='2011-08-03',
        valid_until_exclusive=None,known_at='2011-07-26T23:59:59+08:00',
        contract_multiplier=2039.9985,deliverable_cash_twd=0.,subscription_rights_at_final_settlement=False,
        source_content_sha256s=['c'*64])],schema_overrides={'valid_until_exclusive':pl.String})
    obs=pl.DataFrame(dict(product=['AA1','AA1','AAF'],contract=['201109']*3,
        date=[date(2011,8,3),date(2011,9,21),date(2011,9,21)]))
    law=dict(law_effective_date=date(2010,1,25),law_known_at='2010-01-08T23:59:59+08:00',law_source_sha256='d'*64)
    result=derive_adjusted_final_fixings(obs,final,terms,u,**law)
    assert result.height==1 and result['final_settlement_price'][0]==31.1
    assert result['final_settlement_value'][0] is None
    assert result['fixing_source_product'][0]=='AAF' and result['candidate_only'][0]
    assert result['fixing_corporate_source_sha256s'][0].to_list()==['c'*64]
    merged=pl.concat([final,result],how='diagonal_relaxed')
    _,lives=physical_lifetime_calendar(obs,merged,obs.select('date'))
    assert lives.filter(pl.col('product')=='AA1')['lifetime_status'][0]=='derived_same_security_final'
    assert derive_adjusted_final_fixings(obs,merged,terms,u,**law).is_empty()
    assert derive_adjusted_final_fixings(obs,final.filter(pl.col('product')=='BBF'),terms,u,**law).is_empty()
    ended=terms.with_columns(pl.lit('2011-09-20').alias('valid_until_exclusive'))
    assert derive_adjusted_final_fixings(obs,final,ended,u,**law).is_empty()
    future=terms.with_columns(pl.lit('2011-09-21T23:59:59+08:00').alias('known_at'))
    assert derive_adjusted_final_fixings(obs,final,future,u,**law).is_empty()
    ceased=obs.filter(~((pl.col('product')=='AA1') & (pl.col('date')==date(2011,9,21))))
    assert derive_adjusted_final_fixings(ceased,final,terms,u,**law).is_empty()


def test_zero_oi_delisting_preserves_observed_prices_and_does_not_create_final_settlement():
    from stockagent.data.tw_futures_margin_preparation import physical_lifetime_calendar
    days=[date(2011,9,d) for d in (8,9,12)]
    rows=[]
    for code,oi,asset in [('AA1',0,'stock_future'),('BB1',1,'stock_future'),
                         ('CC1',None,'stock_future'),('DD1',0,'etf_future'),('EEF',0,'stock_future')]:
        rows.extend(dict(product=code,contract='201112',date=day,open_interest=value,
                         asset_class=asset,source_sha256='a'*64)
                    for day,value in zip(days[:2],[7,oi]))
    observed=pl.DataFrame(rows)
    final=pl.DataFrame(schema={'product':pl.String,'contract':pl.String,'settlement_date':pl.Date,
                              'final_settlement_price':pl.Float64,'final_settlement_value':pl.Float64})
    law=dict(law_effective_date=date(2010,1,25),law_known_at='2010-01-08T23:59:59+08:00',law_source_sha256='d'*64)
    calendar,lives=physical_lifetime_calendar(observed,final,pl.DataFrame({'date':days}),delisting_rule=law)
    assert lives.filter(pl.col('lifetime_status')=='zero_open_interest_delisting')['product'].to_list()==['AA1']
    row=lives.filter(pl.col('product')=='AA1').row(0,named=True)
    assert row['delisting_date']==days[-1] and row['calendar_end']==days[1]
    assert row['official_expiry'] is None and row['final_settlement_price'] is None
    assert row['last_observation_source_sha256']=='a'*64
    assert calendar.filter(pl.col('product')=='AA1').height==2
    # Neither a future statute nor a missing OI field proves early delisting.
    later=dict(law,law_effective_date=date(2012,1,1))
    _,later_lives=physical_lifetime_calendar(observed,final,pl.DataFrame({'date':days}),delisting_rule=later)
    assert later_lives['lifetime_status'].eq('unresolved_expiry').all()
    with pytest.raises(ValueError,match='sourced open interest'):
        physical_lifetime_calendar(observed.drop('open_interest'),final,pl.DataFrame({'date':days}),delisting_rule=law)


def test_terminal_values_do_not_drop_binary_rounding_dollars_or_unknown_rights():
    from stockagent.data.tw_futures_margin_preparation import terminal_contract_value_twd,subscription_right_value_twd
    assert terminal_contract_value_twd('1.005',2000)==2010
    assert terminal_contract_value_twd('107.55','2199.9843',0,767)==237375
    assert subscription_right_value_twd('13.75','11.73','196.3466')==396
    assert subscription_right_value_twd('9.34',10,2000)==0
    with pytest.raises(ValueError,match='unresolved'):
        terminal_contract_value_twd(100,2000,0,None)


def test_terminal_input_binding_preserves_missing_rights_and_official_cells():
    from stockagent.data.tw_futures_margin_preparation import bind_adjusted_terminal_values
    terms=pl.DataFrame([dict(product='AA1',contract='201203',effective_date='2012-03-01',
        valid_until_exclusive='2012-03-22',known_at='2012-02-20T23:59:59+08:00',
        contract_multiplier=2000.,deliverable_cash_twd=0.,
        subscription_rights_at_final_settlement=True,source_content_sha256s=['a'*64])])
    final=pl.DataFrame(dict(date=[date(2012,3,21)],product=['AA1'],contract=['201203'],
        final_settlement_price=[1.005],final_settlement_value=[None]),
        schema_overrides={'final_settlement_value':pl.Float64})
    unresolved=bind_adjusted_terminal_values(final,terms)
    assert unresolved['terminal_value_input_twd'][0] is None
    rights=final.select('date','product','contract').with_columns(
        pl.lit(7.).alias('rights_twd'),pl.lit('a'*64).alias('notice_content_sha256'))
    derived=bind_adjusted_terminal_values(final,terms,rights)
    assert derived['terminal_value_input_twd'][0]==2017
    assert derived['official_final_settlement_value'][0] is None
    with pytest.raises(ValueError,match='different notices'):
        bind_adjusted_terminal_values(final,terms,rights.with_columns(
            pl.lit('b'*64).alias('notice_content_sha256')))
    with pytest.raises(ValueError,match='nonnegative'):
        bind_adjusted_terminal_values(final,terms,rights.with_columns(pl.lit(-1.).alias('rights_twd')))
    official=bind_adjusted_terminal_values(final.with_columns(
        pl.lit(2018.).alias('final_settlement_value')),terms)
    assert official['terminal_value_input_twd'][0]==2018
    assert official['terminal_value_binding_status'][0]=='official_full_deliverable'
    assert bind_adjusted_terminal_values(final.head(0),terms).height==0


@pytest.mark.parametrize('close,amount',[('18.2',549.),('14.8',0.)])
def test_source_fixed_rights_need_no_second_fixing_and_count_zero_remainder_once(close,amount):
    from stockagent.data.tw_futures_margin_preparation import corporate_terms_intervals,bind_adjusted_terminal_values
    evidence=dict(product='AA1',observation_date='2013-04-08',closing_price=close,
        subscription_price='15',subscription_quantity='171.8157',rounding='floor_twd',
        rounding_evidence='元以下無條件捨去',derived_value_twd=str(amount),
        covers_all_subscription_components=True)
    fact=dict(product='AA1',from_product='AAF',effective_date='2013-08-19',
        contract_months=['201309'],published_date='2013-08-09',known_at='2013-08-09T23:59:59+08:00',
        issue_date_bound=True,source_content_sha256='a'*64,source_url='synthetic_test',
        contract_multiplier=2140.,deliverable_security_quantity=2140.,deliverable_cash_twd=0.,
        deliverable_components_resolved=True,has_equity_credit_fields=False,
        subscription_rights_at_final_settlement=True,fixed_subscription_rights_twd=amount,
        fixed_subscription_rights_evidence=evidence,visual_review_sha256='b'*64)
    rows,issues=corporate_terms_intervals([fact]);assert not issues
    assert fact['subscription_rights_at_final_settlement'] is True  # source fact is preserved
    assert rows[0]['subscription_rights_at_final_settlement'] is False
    assert rows[0]['deliverable_cash_twd']==0. and rows[0]['fixed_subscription_rights_twd']==amount
    terms=pl.DataFrame(rows,schema_overrides={'valid_until_exclusive':pl.String})
    final=pl.DataFrame(dict(date=[date(2013,9,18)],product=['AA1'],contract=['201309'],
        final_settlement_price=[20.],final_settlement_value=[None]),
        schema_overrides={'final_settlement_value':pl.Float64})
    result=bind_adjusted_terminal_values(final,terms)
    assert result['terminal_value_input_twd'][0]==42800.+amount
    assert result['official_final_settlement_value'][0] is None
    index=final.select('date','product','contract').with_columns(pl.lit(0.).alias('rights_twd'),
        pl.lit('a'*64).alias('notice_content_sha256'))
    equivalent=bind_adjusted_terminal_values(final,terms,index)
    assert equivalent['terminal_value_input_twd'][0]==42800.+amount
    assert equivalent['terminal_value_binding_status'][0]=='source_bound_fixed_subscription_components'
    with pytest.raises(ValueError,match='already fixed'):
        bind_adjusted_terminal_values(final,terms,index.with_columns(pl.lit(amount+1).alias('rights_twd')))
    with pytest.raises(ValueError,match='different notices'):
        bind_adjusted_terminal_values(final,terms,index.with_columns(pl.lit('c'*64).alias('notice_content_sha256')))


def test_fixed_rights_require_complete_prior_source_proof():
    from stockagent.data.tw_futures_margin_preparation import corporate_subscription_rights_require_fixing
    fact=dict(product='AA1',published_date='2013-08-09',subscription_rights_at_final_settlement=True,
        fixed_subscription_rights_twd=549.,deliverable_cash_twd=0.,visual_review_sha256='b'*64)
    assert corporate_subscription_rights_require_fixing(fact)  # amount alone cannot resolve future rights
    proof=dict(product='AA1',observation_date='2013-04-08',closing_price='18.2',
        subscription_price='15',subscription_quantity='171.8157',rounding='floor_twd',
        rounding_evidence='元以下無條件捨去',derived_value_twd='549',
        covers_all_subscription_components=True)
    assert not corporate_subscription_rights_require_fixing(dict(fact,fixed_subscription_rights_evidence=proof))
    assert corporate_subscription_rights_require_fixing(dict(fact,fixed_subscription_rights_evidence={**proof,'covers_all_subscription_components':None}))
    assert corporate_subscription_rights_require_fixing(dict(fact,fixed_subscription_rights_evidence={**proof,'covers_all_subscription_components':False}))
    assert corporate_subscription_rights_require_fixing(dict(fact,fixed_subscription_rights_evidence={**proof,'rounding_evidence':None}))
    for changes in ({'observation_date':'2013-08-09'},{'product':'AB1'},
                    {'closing_price':'19.2'},{'derived_value_twd':'550'},{'rounding':'round_half_up'},
                    {'covers_all_subscription_components':'true'}):
        with pytest.raises(ValueError,match='source-bound fixed'):
            corporate_subscription_rights_require_fixing(dict(fact,fixed_subscription_rights_evidence={**proof,**changes}))


def test_old_fixed_rights_do_not_resolve_a_new_pending_entitlement():
    from stockagent.data.tw_futures_margin_preparation import corporate_terms_intervals,bind_adjusted_terminal_values
    fact=dict(product='AA2',from_product='AA1',effective_date='2020-04-17',
        contract_months=['202006'],published_date='2020-04-13',known_at='2020-04-13T23:59:59+08:00',
        issue_date_bound=True,source_content_sha256='a'*64,source_url='synthetic_test',
        contract_multiplier=2000.,deliverable_security_quantity=2000.,deliverable_cash_twd=0.,
        deliverable_components_resolved=True,has_equity_credit_fields=False,
        subscription_rights_at_final_settlement=True,fixed_subscription_rights_twd=45.,
        visual_review_sha256='b'*64,fixed_subscription_rights_evidence=dict(
            product='AA2',observation_date='2019-09-20',closing_price='9.68',subscription_price='8.6',
            subscription_quantity='41.7146',rounding='floor_twd',rounding_evidence='元以下無條件捨去',
            derived_value_twd='45'))
    rows,issues=corporate_terms_intervals([fact]);assert not issues
    assert rows[0]['subscription_rights_at_final_settlement'] is True
    terms=pl.DataFrame(rows,schema_overrides={'valid_until_exclusive':pl.String})
    final=pl.DataFrame(dict(date=[date(2020,6,17)],product=['AA2'],contract=['202006'],
        final_settlement_price=[8.66],final_settlement_value=[None]),
        schema_overrides={'final_settlement_value':pl.Float64})
    assert bind_adjusted_terminal_values(final,terms)['terminal_value_input_twd'][0] is None
    pending=final.select('date','product','contract').with_columns(
        pl.lit(21.).alias('rights_twd'),pl.lit('a'*64).alias('notice_content_sha256'))
    bound=bind_adjusted_terminal_values(final,terms,pending)
    assert bound['terminal_value_input_twd'][0]==17386.
    assert bound['official_final_settlement_value'][0] is None


def test_same_security_rate_rule_does_not_scale_etf_sums_or_override_ended_direct_evidence():
    from stockagent.data.tw_futures_margin_preparation import bind_equity_margin_families
    u=pl.DataFrame(dict(product=['AAF','AA1','ABF','BBF','BB1'],
        product_name=['甲期貨','甲期貨','小型甲期貨','乙ETF期貨','乙ETF期貨'],
        underlying_symbol=['1000','1000','1000','0050','0050'],
        asset_class=['stock_future']*3+['etf_future']*2))
    rows=[interval(product='AAF',margin_kind='notional_rate',initial=.135,maintenance=.1035),
          interval(product='BBF',initial=40000.,maintenance=31000.),
          interval(product='ABF',margin_kind='notional_rate',initial=.135,maintenance=.1035,
                   valid_until_date_exclusive='2020-01-02',valid_until_phase_exclusive=0)]
    levels=pl.DataFrame(rows,schema_overrides={'valid_until_date_exclusive':pl.String,'valid_until_phase_exclusive':pl.Int64})
    q=pl.DataFrame(dict(date=[date(2020,1,3)]*5,product=u['product']))
    bound=bind_equity_margin_families(q,levels,u,rule_effective_date=date(2010,1,25),
        rule_known_at='2010-01-19T23:59:59+08:00',rule_source_sha256='f'*64)
    assert bound.filter(pl.col('product')=='AA1')['opening_initial'][0]==.135
    assert bound.filter(pl.col('product')=='AA1')['opening_source_product'][0]=='AAF'
    assert bound.filter(pl.col('product').is_in(['ABF','BB1']))['opening_initial'].null_count()==2


@pytest.mark.parametrize('case,expected', [
    ('same_day_rate', 'same_day_clock_review'),
    ('future_publication', 'no_prior_interval'),
    ('fixed_sum', 'no_prior_interval'),
    ('law_not_yet_known', 'no_prior_interval'),
    ('direct_ended', 'interval_ended'),
    ('direct_review', 'same_day_clock_review'),
])
def test_same_security_margin_preserves_same_day_candidate_without_admitting_amount(case, expected):
    from stockagent.data.tw_futures_margin_preparation import bind_equity_margin_families
    u = pl.DataFrame(dict(product=['AAF','AA1'], root_product=['AAF','AAF'],
        product_name=['股票期貨','調整股票期貨'], underlying_symbol=['1000','1000'],
        asset_class=['stock_future','stock_future']))
    parent = interval(product='AAF', margin_kind='notional_rate', initial=.135,
        maintenance=.1035, known_at='2020-01-03T13:35:00+08:00')
    if case == 'future_publication': parent['known_at'] = '2020-01-04T00:00:00+08:00'
    if case == 'fixed_sum': parent.update(margin_kind='fixed_twd', initial=40000., maintenance=31000.)
    rows = [parent]
    if case == 'direct_ended': rows.append(interval(product='AA1',
        margin_kind='notional_rate', initial=.162, maintenance=.1242,
        valid_until_date_exclusive='2020-01-02', valid_until_phase_exclusive=0))
    if case == 'direct_review': rows.append(dict(parent, product='AA1', initial=.162, maintenance=.1242))
    levels = pl.DataFrame(rows, schema_overrides={
        'valid_until_date_exclusive': pl.String, 'valid_until_phase_exclusive': pl.Int64})
    q = pl.DataFrame(dict(date=[date(2020,1,3)], product=['AA1']))
    known = '2020-01-03T08:00:00+08:00' if case == 'law_not_yet_known' else '2010-01-19T23:59:59+08:00'
    row = bind_equity_margin_families(q, levels, u, rule_effective_date=date(2010,1,25),
        rule_known_at=known, rule_source_sha256='f'*64).row(0, named=True)
    for prefix in ('opening_', 'settlement_'):
        assert row[prefix+'binding_status'] == expected
        assert row[prefix+'initial'] is None and row[prefix+'maintenance'] is None
        assert row[prefix+'known_at'] is None
        if case == 'same_day_rate':
            assert row[prefix+'source_product'] == 'AAF'
            assert row[prefix+'margin_interval_id'] == 0
            assert row[prefix+'family_rule_sha256'] == 'f'*64
        else:
            assert row[prefix+'source_product'] == 'AA1'
            assert row[prefix+'family_rule_sha256'] is None


def test_retained_visual_margin_code_correction_survives_reextraction():
    from scripts.build_tw_futures_margin_event_candidates import retain_margin_code_corrections
    row=dict(source_content_sha256='a'*64,source_url='official',product='I5F',margin_kind='fixed_twd',
        after=[22000,17000,16000],before=[25000,19000,18000],source_product_code='ISF',
        product_code_policy='source_bound_visual_cell_correction',product_code_review_sha256='b'*64)
    broken=dict(row,product='ISF');genuine=dict(broken,source_content_sha256='c'*64)
    assert retain_margin_code_corrections([row],[broken,genuine])==[genuine]


def test_old_corporate_position_text_requires_exact_scoped_amounts_and_dates():
    from stockagent.data.tw_futures_margin_preparation import corporate_position_text_candidates
    fact=dict(from_product='JJF',product='JJ1',effective_date='2011-08-15',
              contract_multiplier=2239.2845,issue_date_bound=True)
    source=('加掛標準契約：契約代號 JJF 約定標的物 2,000 股標的證券 '
            '部位限制：每口折算股數 JJF 2,000 JJ1 2,239.2845 '
            'JJ1 與 JJF 部位合併計算。部位限制數：'
            '自 100.8.15 起至 100.9.21 止，自 100.9.22 起至 JJ1 契約終止掛牌前一營業日止 '
            '自然人 783,750 股 700,000 股 法人機構')
    rows=corporate_position_text_candidates(source,[fact])
    assert len(rows)==4
    assert rows[1]['position_unit']==2239.2845
    assert rows[0]['natural_person_limit']==783750
    assert rows[-1]['requires_delisting_clock'] is True
    assert corporate_position_text_candidates(source.replace('700,000','7O0,000'),[fact])==[]
    gap=corporate_position_text_candidates(source.replace('100.9.22','100.9.23'),[fact])
    assert gap[0]['valid_until_date_inclusive']=='2011-09-21'
    assert gap[2]['effective_date']=='2011-09-23'  # Never invent a level in the gap.
    assert corporate_position_text_candidates(source,[fact,dict(fact,product='JJ2')])==[]


def test_flat_corporate_table_preserves_merger_codes_units_and_excludes_options():
    from stockagent.data.tw_futures_margin_preparation import corporate_position_text_candidates
    facts=[dict(from_product=old,product=code,effective_date='2021-01-06',
        contract_multiplier=units,issue_date_bound=True)
        for old,code,units in [('DUF','QB1',1000.),('MCF','QB2',550.)]]
    source=('二、部位限制：自110年1月6日起至QBA、QB1、QB2契約終止掛牌前一營業日止 '
        '持有部位 QBO QBA QBF QB1 QB2\n每口折算股數 2,000 1,000 2,000 1,000 550\n'
        '[PAGE 4] (二)部位合併計算：QB1、QB2與QBF部位合併計算；QBA與QBO同方向選擇權部位合併計算。'
        '(三)部位限制數：自然人 法人/期貨自營商 造市者\n16,000,000股 48,000,000股 120,000,000股')
    rows=corporate_position_text_candidates(source,facts)
    assert {r['product']:r['position_unit'] for r in rows}=={'QBF':2000.,'QB1':1000.,'QB2':550.}
    assert all(r['natural_person_limit']==16000000. and r['requires_delisting_clock'] for r in rows)
    assert corporate_position_text_candidates(source.replace('1,000 550','1,000 55O'),facts)==[]
    assert corporate_position_text_candidates(source,[dict(facts[0],contract_multiplier=1100.),facts[1]])==[]


def test_flat_two_period_cap_rejects_lost_row_or_column_ownership():
    from stockagent.data.tw_futures_margin_preparation import corporate_position_text_candidates
    fact=dict(from_product='JNF',product='JN1',effective_date='2011-08-02',
              contract_multiplier=2100.,issue_date_bound=True)
    heading=('加掛標準契約：契約代號 JNF 約定標的物 2,000 股標的證券 '
        '部位限制：持有部位 JNF JN1\n每口折算股數 2,000 2,100\n'
        'JN1 與 JNF 部位合併計算。部位限制數：'
        '自100.8.2起至100.9.21止，自100.9.22起至JN1契約終止掛牌前一營業日止\n')
    # The real natural-person amounts precede their label in this OCR view;
    # flattening would lend the following institutional amounts to that label.
    lost_row=heading+'735,000股700,000股\n自然人\n2,625,000股2,500,000股\n法人機構'
    assert corporate_position_text_candidates(lost_row,[fact])==[]
    # Separate cell lines have no retained left/right date ownership either.
    lost_columns=heading+'自然人\n700,000股\n735,000股\n法人機構'
    assert corporate_position_text_candidates(lost_columns,[fact])==[]
    owned=heading+'自然人 735,000股 700,000股 法人機構'
    rows=corporate_position_text_candidates(owned,[fact])
    assert len(rows)==4 and rows[0]['natural_person_limit']==735000
    assert rows[-1]['natural_person_limit']==700000


def test_retained_cap_grid_keeps_person_and_date_columns_with_ocr_labels():
    from stockagent.data.tw_futures_margin_preparation import corporate_position_table_candidates
    # Retained CJ1 announcement: the options columns do not become futures,
    # and institutional allowances do not become natural-person allowances.
    facts=[dict(from_product='CJF',product='CJ1',effective_date='2011-08-11',
                contract_multiplier=2120.,issue_date_bound=True)]
    pages=[dict(page=3,native_text='',ocr_text='CJ1與CJF部位合併计算',tables=[
        dict(cells=[['持有部位','CJO','CJA','CJF','CJ1'],
                    ['每口折算股数','2,000','2,120','2,000','2,120']]),
        dict(cells=[['適用期間\n部位限制数',
                     '自100.08.11起至100.09.21(100年9月契約到期日)止',
                     '自100.09.22起至CJA、CJ1契约终止掛牌前一营业日止'],
                    ['法人機構','7,950,000股','7,500,000股'],
                    ['自然人','2,650,000股','2,500,000股']])])]
    rows=corporate_position_table_candidates(pages,corporate=facts)
    assert len(rows)==4
    assert {r['product'] for r in rows}=={'CJF','CJ1'}
    first=[r for r in rows if r['effective_date']=='2011-08-11']
    assert all(r['natural_person_limit']==2650000 and
               r['valid_until_date_inclusive']=='2011-09-21' for r in first)
    second=[r for r in rows if r['effective_date']=='2011-09-22']
    assert all(r['natural_person_limit']==2500000 and r['requires_delisting_clock'] for r in second)
    assert corporate_position_table_candidates(pages,
        corporate=[dict(facts[0],contract_multiplier=2200.)])==[]


def test_flat_single_adjusted_cap_requires_matching_source_units():
    from stockagent.data.tw_futures_margin_preparation import corporate_position_text_candidates
    fact=dict(from_product='KQF',product='KQ1',effective_date='2012-08-13',
        contract_multiplier=2060.,issue_date_bound=True)
    source=('二、部位限制：持有部位 KQ1\n每口折算股數 2,060\n(二)部位限制數：'
        '自然人 法人機構 造市者\n721,000股 2,575,000股 6,386,000股')
    rows=corporate_position_text_candidates(source,[fact])
    assert len(rows)==1 and rows[0]['position_unit']==2060.
    assert rows[0]['combined_products']==['KQ1']
    assert rows[0]['natural_person_limit']==721000. and rows[0]['requires_delisting_clock']
    assert corporate_position_text_candidates(source.replace('2,060','2.060'),[fact])==[]


def test_position_only_cells_match_independent_terms_and_keep_explicit_start():
    from stockagent.data.tw_futures_margin_preparation import corporate_position_table_candidates
    terms=[dict(from_product='AAF',product='AA1',contract_multiplier=1600.)]
    pages=[dict(page=2,native_text='AA1與AAF部位合併計算。',tables=[
        dict(cells=[['持有部位','AAF','AA1'],['每口折算股數','2,000','1,600']]),
        dict(cells=[['適用期間','自100年1月7日起改按標的證券股數計算'],['自然人','7,500,000股']]),
    ])]
    rows=corporate_position_table_candidates(pages,corporate=terms)
    assert len(rows)==2 and all(r['effective_date']=='2011-01-07' for r in rows)
    assert all(r['end_rule']=='until_superseding_rule' and not r['requires_delisting_clock'] for r in rows)
    assert corporate_position_table_candidates(pages,corporate=[dict(terms[0],contract_multiplier=160.)])==[]
    assert corporate_position_table_candidates(pages,corporate=[])==[]


def test_quarterly_position_clock_separates_notice_raise_and_later_reduction():
    from stockagent.data.tw_futures_margin_preparation import position_grid_candidates
    cells=[['契約名稱','商品代碼','自然人'],['甲期貨','AA','4,000']]
    source=('股票期貨 單位：契約數。調整本公司股票期貨部位限制數。'
        '前揭部位限制數為調高者，自公告之次一營業日一般交易時段起生效；'
        '為調降者，自次近月份契約到期後次一營業日一般交易時段生效（即107年12月20日）。'
        '註1：部位限制數之提高，皆自公告日起生效。')
    market=[date(2018,10,19),date(2018,10,22),date(2018,12,20)]
    raised=position_grid_candidates(cells,source,caption='提高部位限制數之契約',
        published_date='2018-10-19',market_dates=market)
    lowered=position_grid_candidates(cells,source,caption='降低部位限制數之契約',
        published_date='2018-10-19',market_dates=market)
    assert raised[0]['effective_date']=='2018-10-22'
    assert lowered[0]['effective_date']=='2018-12-20'
    referenced=position_grid_candidates(cells,source,caption='股票期貨（註1）',
        published_date='2018-10-19',market_dates=market)
    assert referenced[0]['effective_date']=='2018-10-22'
    unknown=position_grid_candidates(cells,source,caption='股票期貨（註3）',
        published_date='2018-10-19',market_dates=market)
    assert unknown[0]['effective_date'] is None
    assert position_grid_candidates(cells,source,caption='提高部位限制數之契約',
        published_date='2018-10-19')[0]['effective_date'] is None


def test_identical_integer_and_float_cap_views_are_not_conflicting_notices():
    from stockagent.data.tw_futures_margin_preparation import position_candidate_intervals
    row=dict(product='AA1',effective_date='2020-01-01',effective_phase='product_regular_open',
        known_at='2019-12-20T23:59:59+08:00',issue_date_bound=True,source_content_sha256='a'*64,
        source_url='official-test',event_type='corporate_securities_unit_limit',unit='shares',
        position_unit=2000,natural_person_limit=4000000,combined_products=['AA1','AAF'])
    intervals,issues=position_candidate_intervals([row,dict(row,position_unit=2000.,natural_person_limit=4000000.)])
    assert not issues and len(intervals)==1 and intervals[0]['position_limit']==4000000.


def test_old_immediate_position_notice_and_explicit_split_dates():
    from stockagent.data.tw_futures_margin_preparation import position_grid_candidates
    cells=[['商品契約','商品代碼','自然人'],['甲期貨','AA','2,500']]
    old='主旨：公告調整股票期貨契約交易人部位限制數，並自即日起實施。依據：交易規則。單位：契約數'
    assert position_grid_candidates(cells,old,published_date='2010-07-23')[0]['effective_date']=='2010-07-23'
    split=('調整本公司股票期貨部位限制數調高者，自109年10月22日起生效；'
           '調降者，自109年12月17日起生效。單位：契約數')
    assert position_grid_candidates(cells,split,caption='降低部位限制數之契約',
        published_date='2020-10-21')[0]['effective_date']=='2020-12-17'
    # Never borrow an index-option date for the separate stock paragraph.
    mixed=('台指選擇權自然人上限自109年12月17日起生效。調整本公司股票期貨，'
           '部位限制數调降者，自次近月份到期後生效。單位：契約數')
    assert position_grid_candidates(cells,mixed,caption='降低部位限制數之契約',
        published_date='2020-10-21')[0]['effective_date'] is None


def test_named_later_reduction_does_not_delay_other_products():
    from stockagent.data.tw_futures_margin_preparation import position_grid_candidates
    cells=[['契約名稱','契約代碼','自然人'],
        ['華票期貨','HX','350'],['台中銀期貨','HW','350']]
    text=('股票期貨單位：契約數。前揭契約部位限制數為調高者，自公告日起生效；'
        '為調降者，除華票期貨自公告日該期貨已上市契約均到期後生效'
        '（即102年9月19日）外，其餘皆自公告日起生效。')
    rows=position_grid_candidates(cells,text,caption='降低部位限制數之契約',published_date='2013-01-15')
    assert {r['product']:r['effective_date'] for r in rows}=={'HXF':'2013-09-19','HWF':'2013-01-15'}


@pytest.mark.parametrize('name',['級數','級距'])
def test_quarterly_latest_grade_clause_preserves_both_original_wordings(name):
    from stockagent.data.tw_futures_margin_preparation import position_grid_candidates
    text=('股票期貨；單位：契約數。發文日期：中華民國100年11月11日。'
        '主旨：公告調整股票期貨部位限制數。依據：交易規則。'
        '本公告自即日起生效。本次部位限制數調整生效後，則依其最新適用之部位限制'+name+'計算。')
    cells=[['契約代碼','商品契約','自然人'],['GL','正崴期貨','1,250']]
    parsed=position_grid_candidates(cells,text,published_date='2011-11-11')
    assert len(parsed)==1 and parsed[0]['limit_follows_applicable_grade']
    earlier=position_grid_candidates(cells,text.replace('調整生效後','調整生效前'),published_date='2011-11-11')
    assert len(earlier)==1 and not earlier[0]['limit_follows_applicable_grade']


def test_equivalent_listing_and_combined_caps_keep_group_and_shortest_validity():
    from stockagent.data.tw_futures_margin_preparation import position_candidate_intervals
    common=dict(product='AAF',effective_date='2020-01-01',effective_phase='product_regular_open',
        known_at='2019-12-20T23:59:59+08:00',issue_date_bound=True,source_url='official-test')
    absolute=dict(common,source_content_sha256='a'*64,event_type='absolute_level',
        unit='contracts',natural_person_limit=2000,valid_until_date_inclusive='2020-02-01')
    combined=dict(common,source_content_sha256='b'*64,event_type='corporate_securities_unit_limit',
        unit='shares',position_unit=2000,natural_person_limit=4000000,
        combined_products=['AA1','AAF'],requires_delisting_clock=True,
        end_rule='previous_business_day_before_contract_delisting')
    intervals,issues=position_candidate_intervals([absolute,combined])
    assert not issues and len(intervals)==1
    row=intervals[0]
    assert row['position_limit']==4000000 and row['position_unit']==2000
    assert row['combined_products']==['AA1','AAF'] and row['requires_delisting_clock']
    assert row['valid_until_date_exclusive']=='2020-02-02'
    assert row['source_content_sha256s']==['a'*64,'b'*64]
    # The combined cap already implies a looser standalone cap; no obligation
    # is discarded by keeping its complete combined group.
    implied,issues=position_candidate_intervals([dict(absolute,natural_person_limit=2001),combined])
    assert not issues and implied[0]['position_limit']==4000000
    assert implied[0].get('independent_contract_limit') is None


def test_independent_contract_cap_keeps_the_full_combined_share_obligation():
    from stockagent.data.tw_futures_margin_preparation import position_candidate_intervals
    common=dict(product='AAF',effective_date='2020-01-01',effective_phase='product_regular_open',
        known_at='2019-12-20T23:59:59+08:00',issue_date_bound=True,source_url='official-test')
    absolute=dict(common,source_content_sha256='a'*64,event_type='absolute_level',
        unit='contracts',natural_person_limit=350)
    combined=dict(common,source_content_sha256='b'*64,event_type='corporate_securities_unit_limit',
        unit='shares',position_unit=2000,natural_person_limit=2500000,
        combined_products=['AA1','AAF'],requires_delisting_clock=True)
    rows,issues=position_candidate_intervals([absolute,combined])
    assert not issues and len(rows)==1
    assert rows[0]['position_unit']==2000 and rows[0]['position_limit']==2500000
    assert rows[0]['independent_contract_limit']==350
    assert rows[0]['source_content_sha256s']==['a'*64,'b'*64]
    # Contradictory copies of the same obligation and a third independent
    # monthly cap cannot be compressed into two constraints.
    for extra in [dict(absolute,source_content_sha256='c'*64,natural_person_limit=500),
                  dict(absolute,natural_person_monthly_limit=100),
                  dict(absolute,limit_follows_applicable_grade=True)]:
        facts=[absolute,combined,extra] if extra['source_content_sha256']=='c'*64 else [extra,combined]
        rejected,errors=position_candidate_intervals(facts)
        assert rejected==[] and any('conflicting' in v['reasons'] for v in errors)


def test_same_unit_adjusted_combination_cannot_be_invented_for_changed_shares():
    from stockagent.data.tw_futures_margin_preparation import (
        corporate_position_text_candidates,position_candidate_intervals)
    fact=dict(from_product='CVF',product='CV1',effective_date='2010-07-12',
              contract_multiplier=2000.,issue_date_bound=True)
    source='加掛標準契約 約定標的物 2,000 股標的證券。部位限制：標準契約與調整契約部位合併計算。'
    rows=corporate_position_text_candidates(source,[fact])
    assert len(rows)==1 and rows[0]['combined_position_ratio']=='1/1'
    row=dict(rows[0],issue_date_bound=True,known_at='2010-06-30T23:59:59+08:00',
             source_content_sha256='a'*64,source_url='official')
    intervals,issues=position_candidate_intervals([row])
    assert len(intervals)==1 and not issues
    assert intervals[0]['position_limit'] is None  # still requires the dated root
    assert corporate_position_text_candidates(source,[dict(fact,contract_multiplier=2200.)])==[]
    _,bad=position_candidate_intervals([dict(row,extraction_method='unknown')])
    assert bad


def test_named_multiple_generations_bind_only_the_unchanged_member():
    from stockagent.data.tw_futures_margin_preparation import (
        corporate_position_text_candidates, position_candidate_intervals)
    facts = [dict(from_product='HSF', product='HS1', effective_date='2018-12-05',
                  contract_multiplier=2000., issue_date_bound=True),
             dict(from_product='HS1', product='HS2', effective_date='2018-12-05',
                  contract_multiplier=2100., issue_date_bound=True)]
    source = ('加掛標準契約 契約代號 HSF 約定標的物 2,000 股標的證券。'
              '部位限制：HSF、HS1與HS2部位合併計算。')
    rows = corporate_position_text_candidates(source, facts)
    assert len(rows) == 1 and rows[0]['product'] == 'HS1'
    assert rows[0]['combined_position_ratio'] == '1/1'
    assert rows[0]['natural_person_limit'] is None
    bound = dict(rows[0], issue_date_bound=True, known_at='2018-12-03T23:59:59+08:00',
        source_content_sha256='a'*64, source_url='official')
    intervals, issues = position_candidate_intervals([bound])
    assert not issues and len(intervals) == 1
    assert intervals[0]['requires_base_limit_join']
    assert intervals[0]['combined_position_base_product'] == 'HSF'


def test_covering_letter_heading_does_not_hide_the_bound_group_annex():
    from stockagent.data.tw_futures_margin_preparation import corporate_position_text_candidates
    facts = [dict(from_product='FEF', product='FE1', effective_date='2020-09-11',
                  contract_multiplier=2000., issue_date_bound=True),
             dict(from_product='FE1', product='FE2', effective_date='2020-09-11',
                  contract_multiplier=2040., issue_date_bound=True)]
    annex = ('二、加掛標準契約：契約代號FEF約定標的物2,000股標的證券。'
             '三、部位限制：FEF、FE1與FE2部位合併計算。')
    source = '說明：契約調整、加掛標準契約與部位限制等詳如附件。' + annex
    rows = corporate_position_text_candidates(source, facts)
    assert len(rows) == 1 and rows[0]['product'] == 'FE1'
    assert rows[0]['combined_position_ratio'] == '1/1'
    assert rows[0]['natural_person_limit'] is None
    # Two complete but contradictory annexes cannot select the nicer view.
    assert corporate_position_text_candidates(source + annex.replace('2,000', '100'), facts) == []
    # A damaged group is not repaired from a different occurrence's units.
    assert corporate_position_text_candidates(source.replace('與FE2', '與FEI'), facts) == []


@pytest.mark.parametrize('problem', ['changed_units', 'ambiguous_units', 'different_day',
    'unknown_member', 'unbound_identity', 'wrong_standard', 'numeric_cap', 'unit_grid'])
def test_multiple_generation_position_group_requires_exact_source_ownership(problem):
    from stockagent.data.tw_futures_margin_preparation import corporate_position_text_candidates
    facts = [dict(from_product='DNF', product='DN1', effective_date='2014-04-18',
                  contract_multiplier=2000., issue_date_bound=True),
             dict(from_product='DN1', product='DN2', effective_date='2014-04-18',
                  contract_multiplier=2200., issue_date_bound=True)]
    source = ('加掛標準契約 契約代號 DNF 約定標的物 2,000 股標的證券。'
              '部位限制：DNF、DN1與DN2部位合併計算。')
    if problem == 'changed_units': facts[0]['contract_multiplier'] = 2200.
    elif problem == 'ambiguous_units': facts.append(dict(facts[0], contract_multiplier=2100.))
    elif problem == 'different_day': facts[1]['effective_date'] = '2014-04-19'
    elif problem == 'unknown_member': source = source.replace('與DN2', '、DN2與DN3')
    elif problem == 'unbound_identity': facts[1]['issue_date_bound'] = False
    elif problem == 'wrong_standard': source = source.replace('契約代號 DNF', '契約代號 DEF')
    elif problem == 'numeric_cap': source += '自然人部位限制數7,500,000股。'
    else: source += '每口折算股數：2,000、2,000、2,200。'
    assert corporate_position_text_candidates(source, facts) == []


def test_combined_cap_grid_covers_standard_mini_and_transposed_old_tables():
    from stockagent.data.tw_futures_margin_preparation import corporate_position_table_candidates
    adjustments=[['契約代號','PVF調整為PV1'],['契約乘數','PV1契約乘數調整為5965.5892'],
                 ['約定標的物','5965.5892股標的證券']]
    pages=[dict(page=1,native_text='PVF、PV1、PWF與PW1部位合併計算',tables=[
        dict(cells=adjustments),
        dict(cells=[['持有部位','PVF','PV1','PWF','PW1'],
                    ['每口折算股數','2000','5965.5892','100','298.2795']]),
        dict(cells=[['','自然人','法人機構','造市者'],
                    ['部位限制數','11,931,179股','35,793,536股','89,483,838股']],
             caption='適用期間：自115年9月2日起至115年10月21日止。')])]
    rows=corporate_position_table_candidates(pages)
    assert {r['product'] for r in rows}=={'PVF','PV1','PWF','PW1'}
    assert {r['natural_person_limit'] for r in rows}=={11931179}
    assert {r['valid_until_date_inclusive'] for r in rows}=={'2026-10-21'}
    assert next(r['position_unit'] for r in rows if r['product']=='PW1')==298.2795
    pages[0]['tables'][-1]['caption']='適用期間另訂'
    assert corporate_position_table_candidates(pages)==[]
    pages[0]['native_text']=('部位限制：自115年9月2日起至PV1及PW1契約終止掛牌前一營業日止。'
                             'PVF、PV1、PWF與PW1部位合併計算')
    pages[0]['tables'][-1]['cells']=[['自然人','法人機構'],['4,000,000股','12,000,000股']]
    rows=corporate_position_table_candidates(pages)
    assert len(rows)==4 and all(r['requires_delisting_clock'] for r in rows)
    assert {r['natural_person_limit'] for r in rows}=={4000000}


@pytest.mark.parametrize('failure',[None,'halt','extension','intervening_event','missing_source',
                                     'referenced_before','referenced_after_only','referenced_future','invalid_target'])
def test_restoration_requires_completed_observed_disposition_and_preserves_amounts(failure):
    from stockagent.data.tw_futures_margin_preparation import disposal_margin_restorations
    u=pl.DataFrame(dict(product=['SAF'],underlying_symbol=['1795']))
    f=dict(product='SAF',requires_reversion_review=True,issue_date_bound=True,
        before=[.162,.1242,.12],after=[.243,.1863,.18],
        restoration_rule='return_to_declared_before',effective_date='2025-10-03',
        source_content_sha256='a'*64,published_date='2025-10-02',
        known_at='2025-10-02T23:59:59+08:00',temporary_end_evidence=json.dumps([
            dict(date_iso='2025-10-07',boundary='after_regular_session')]))
    d=dict(stock_id='1795',date='2025-10-02',period_start='2025-10-03',period_end='2025-10-07',
           measure='自114年10月3日起3個營業日',source_sha256='b'*64)
    days=['2025-10-03','2025-10-06','2025-10-07']
    if failure=='halt':days.pop(1)
    ds=[d];facts=[f]
    if failure=='extension':ds.append(dict(d,period_start='2025-10-06',period_end='2025-10-08'))
    if failure=='intervening_event':facts.append(dict(product='SAF',effective_date='2025-10-06'))
    if failure=='missing_source':ds=[dict(d,source_sha256=None)]
    if failure in ('referenced_before','referenced_after_only','referenced_future','invalid_target'):
        f.update(restoration_rule='return_to_referenced_before',restoration_target=[.135,.1035,.1],
            restoration_reference_evidence=json.dumps(dict(referenced_effective_date='2025-10-01',
                source_content_sha256s=['d'*64],known_at='2025-09-30T23:59:59+08:00')))
        if failure=='referenced_after_only':f['before']=None
        if failure=='referenced_future':
            proof=json.loads(f['restoration_reference_evidence']);proof['known_at']='2025-10-04T23:59:59+08:00'
            f['restoration_reference_evidence']=json.dumps(proof)
        if failure=='invalid_target':f['restoration_target']=[float('nan'),.1,.1]
    obs=pl.DataFrame(dict(date=days,symbol=['1795']*len(days),volume=[100.]*len(days),source_sha256=['c'*64]*len(days)))
    disposition=pl.DataFrame(ds,schema_overrides={'source_sha256':pl.String})
    if failure=='missing_source':
        with pytest.raises(ValueError,match='source identities'):
            disposal_margin_restorations(facts,u,disposition,obs)
        return
    if failure in ('referenced_future','invalid_target'):
        with pytest.raises(ValueError,match='restoration'):
            disposal_margin_restorations(facts,u,disposition,obs)
        return
    rows,issues=disposal_margin_restorations(facts,u,disposition,obs)
    if failure and failure not in ('referenced_before','referenced_after_only'):
        assert rows==[] and len(issues)==1
    else:
        assert len(rows)==1 and not issues
        assert rows[0]['after']==([.135,.1035,.1] if failure in ('referenced_before','referenced_after_only') else [.162,.1242,.12])
        assert rows[0]['effective_date']=='2025-10-07'
        assert rows[0]['known_at']=='2025-10-07T13:35:00+08:00'
        assert rows[0]['original_rule_known_at']==f['known_at']
        assert rows[0]['point_in_time_verified'] is False


def test_corporate_binding_respects_old_code_reuse_and_optional_fixed_rights():
    from stockagent.data.tw_futures_margin_preparation import bind_dated_corporate_terms
    terms=pl.DataFrame([dict(product='NX1',contract='201412',effective_date='2014-09-30',
        valid_until_exclusive='2014-12-18',known_at='2014-08-28T23:59:59+08:00',
        contract_multiplier=1700.7278,deliverable_cash_twd=0.,fixed_subscription_rights_twd=None,
        subscription_rights_at_final_settlement=False,source_content_sha256s=['a'*64])],
        schema_overrides={'fixed_subscription_rights_twd':pl.Float64})
    q=pl.DataFrame(dict(date=[date(2014,9,29),date(2014,9,30),date(2014,12,18)],
                        product=['NX1']*3,contract=['201412']*3))
    bound=bind_dated_corporate_terms(q,terms)
    assert bound['contract_multiplier'].to_list()==[None,1700.7278,None]
    assert bound['fixed_subscription_rights_twd'].to_list()==[None,0.,None]
    bad=terms.with_columns(pl.lit(-1.).alias('deliverable_cash_twd'))
    with pytest.raises(ValueError,match='amounts'):
        bind_dated_corporate_terms(q,bad)


def test_family_identity_never_invents_an_adjusted_multiplier_or_equates_different_securities():
    from stockagent.data.tw_futures_margin_preparation import equity_contract_families
    u=pl.DataFrame(dict(product=['MYF','OMF','OM1'],product_name=['精華期貨','小型精華期貨','小型精華期貨'],
        asset_class=['stock_future']*3,underlying_symbol=['1565']*3))
    f=equity_contract_families(u).sort('product')
    assert f['standard_product'].to_list()==['MYF']*3
    assert f.filter(pl.col('product')=='OM1')['root_product'][0]=='OMF'
    assert 'contract_multiplier' not in f.columns
    with pytest.raises(ValueError,match='mismatch'):
        equity_contract_families(u.with_columns(pl.when(pl.col('product')=='OM1')
            .then(pl.lit('3008')).otherwise(pl.col('underlying_symbol')).alias('underlying_symbol')))


def test_position_binding_never_revives_ended_limits_or_exposes_same_day_notice():
    from stockagent.data.tw_futures_margin_preparation import bind_dated_position_limits
    t=pl.DataFrame([dict(product='X',effective_date='2020-01-01',valid_until_date_exclusive='2020-01-04',
        known_at='2020-01-02T23:59:59+08:00',admission_not_before=None,
        source_content_sha256s=['a'*64],position_limit=2000)],
        schema_overrides={'admission_not_before':pl.String})
    q=pl.DataFrame(dict(product=['X']*3,date=[date(2020,1,d) for d in (2,3,4)]))
    r=bind_dated_position_limits(q,t)
    assert r['position_limit'].to_list()==[None,2000,None]


def test_dated_combination_binds_unobserved_parent_without_reviving_its_gap():
    from stockagent.data.tw_futures_margin_preparation import (
        bind_dated_position_combinations,position_candidate_intervals)
    proof=dict(issue_date_bound=True,source_content_sha256='a'*64,source_url='official',
        known_at='2019-12-20T23:59:59+08:00',effective_date='2020-01-01',
        effective_phase='product_regular_open',unit='contracts')
    facts=[dict(proof,product='TX',event_type='absolute_level',natural_person_limit=1000,
                valid_until_date_inclusive='2020-01-03'),
           dict(proof,product='MTX',event_type='combined_position_formula',
                combined_position_base_product='TX',combined_position_ratio='1/4')]
    intervals,issues=position_candidate_intervals(facts)
    assert not issues
    levels=pl.DataFrame(intervals,infer_schema_length=None,schema_overrides={
        'admission_not_before':pl.String,'monthly_position_limit':pl.Float64})
    days=pl.DataFrame(dict(date=[date(2020,1,2),date(2020,1,4)],product=['MTX']*2))
    bound=bind_dated_position_combinations(days,levels)
    assert bound['position_limit'].to_list()==[1000,None]
    assert bound['position_unit'][0]==.25
    assert bound['position_root_product'][0]=='TX'
    assert bound['position_binding_status'].to_list()==[
        'bound_dated_combination','base_position_limit_unresolved']
    future=dict(proof,product='TX',event_type='absolute_level',natural_person_limit=20000,
                effective_date='2020-02-01')
    later,_=position_candidate_intervals([*facts,future])
    extended=bind_dated_position_combinations(days,pl.DataFrame(later,infer_schema_length=None,
        schema_overrides={'admission_not_before':pl.String,'monthly_position_limit':pl.Float64}))
    assert extended['position_limit'].equals(bound['position_limit'])


def test_security_position_family_never_guesses_changed_share_caps():
    from stockagent.data.tw_futures_margin_preparation import bind_equity_position_families,position_candidate_intervals
    u=pl.DataFrame(dict(product=['AAF','AA1','ABF','BBF','BB1'],
        product_name=['甲期貨','甲期貨','小型甲期貨','乙期貨','乙期貨'],
        underlying_symbol=['1000']*3+['2000']*2,asset_class=['stock_future']*5))
    proof=dict(issue_date_bound=True,source_content_sha256='a'*64,source_url='official',
        known_at='2019-12-20T23:59:59+08:00',effective_date='2020-01-01',
        effective_phase='product_regular_open',unit='contracts',natural_person_limit=1000)
    rows,_=position_candidate_intervals([dict(proof,product=code) for code in ['AAF','BBF']])
    intervals=pl.DataFrame(rows,infer_schema_length=None,schema_overrides={
        'admission_not_before':pl.String,'valid_until_date_exclusive':pl.String,
        'combined_position_base_product':pl.String})
    d=date(2020,1,3)
    terms=pl.DataFrame(dict(date=[d,d],product=['AA1','BB1'],contract=['202003']*2,
        terms_binding_status=['bound_prior_publication']*2,contract_multiplier=[2200.,2000.],
        source_content_sha256s=[['b'*64],['c'*64]]))
    law=dict(effective_date='2020-01-01',known_at='2019-12-20T23:59:59+08:00',
        standard_units=2000,mini_units=100,asset_class='stock_future',source_content_sha256='f'*64)
    bound=bind_equity_position_families(pl.DataFrame(dict(date=[d]*5,product=u['product'])),
        intervals,u,terms,[law])
    assert bound.filter(pl.col('product')=='AA1')['position_limit'][0] is None
    assert bound.filter(pl.col('product')=='ABF')['position_unit'][0]==.05
    assert bound.filter(pl.col('product')=='BB1')['position_unit'][0]==1.
    assert bound.filter(pl.col('product')=='BB1')['position_limit'][0]==1000
    assert bound.filter(pl.col('product')=='BB1')['position_binding_status'][0]=='bound_same_security_position'


@pytest.mark.parametrize('problem', [None, 'ended', 'unknown_end', 'future_publication',
    'unreviewed_law', 'future_law', 'latest_grade_clause', 'conflicting_group',
    'active_original', 'reused_code', 'changed_unit', 'future_unit_clock',
    'finite_same_original', 'finite_reused_code','stale_donor_grade','composed_donor_grade'])
def test_named_corporate_group_retains_literal_share_axis_after_standalone_grade(problem):
    from stockagent.data.tw_futures_margin_preparation import bind_equity_position_families,position_candidate_intervals
    d=date(2020,2,3)
    universe=pl.DataFrame(dict(product=['AAF','AA1','AA2'],product_name=['甲期貨']*3,
        underlying_symbol=['1000']*3,asset_class=['stock_future']*3))
    proof=dict(issue_date_bound=True,source_content_sha256='a'*64,source_url='corporate-original',
        known_at='2019-12-20T23:59:59+08:00',effective_date='2020-01-01',
        effective_phase='product_regular_open')
    until='2020-06-17' if problem!='ended' else '2020-01-31'
    corporate=dict(proof,product='AA1',event_type='corporate_securities_unit_limit',unit='shares',
        position_unit=2100.,natural_person_limit=6_000_000.,combined_products=['AAF','AA1'],
        valid_until_date_inclusive=until)
    if problem in ['stale_donor_grade','composed_donor_grade']:
        corporate['limit_follows_applicable_grade']=True
        if problem=='composed_donor_grade':corporate['position_grade_source_sha256s']=['b'*64]
    if problem in ['unknown_end','active_original','reused_code','changed_unit','future_unit_clock']:
        corporate.pop('valid_until_date_inclusive')
    if problem=='future_publication':corporate['known_at']='2020-02-03T23:59:59+08:00'
    grade=dict(proof,product='AAF',source_content_sha256='b'*64,source_url='quarter-original',
        effective_date='2020-02-01',known_at='2020-01-30T23:59:59+08:00',
        event_type='absolute_level',unit='contracts',natural_person_limit=2500.,
        limit_follows_applicable_grade=problem=='latest_grade_clause')
    facts=[corporate,grade]
    if problem=='conflicting_group':
        facts.append(dict(corporate,product='AA2',natural_person_limit=5_000_000.,position_unit=2200.,
            combined_products=['AAF','AA2']))
    rows,_=position_candidate_intervals(facts)
    intervals=pl.DataFrame(rows,infer_schema_length=None,schema_overrides={
        'admission_not_before':pl.String,'combined_position_base_product':pl.String,
        'valid_until_date_exclusive':pl.String,'monthly_position_limit':pl.Float64,
        'independent_contract_limit':pl.Float64})
    # No adjusted quote is present in this request. A finite, source-bound
    # group interval still owns the standard member; it creates no peer quote.
    days=pl.DataFrame(dict(date=[d],product=['AAF']))
    terms=pl.DataFrame(schema={'date':pl.Date,'product':pl.String,'contract':pl.String,
        'terms_binding_status':pl.String,'contract_multiplier':pl.Float64,
        'source_content_sha256s':pl.List(pl.String)})
    if problem in ['active_original','reused_code','changed_unit','future_unit_clock',
                   'finite_same_original','finite_reused_code']:
        terms=pl.DataFrame([dict(date=d,product='AA1',contract='202006',
            terms_binding_status='bound_prior_publication',
            contract_multiplier=2200. if problem=='changed_unit' else 2100.,
            source_content_sha256s=['c'*64 if problem in ['reused_code','finite_reused_code'] else 'a'*64],
            known_at='2020-02-03T23:59:59+08:00' if problem=='future_unit_clock' else proof['known_at'])])
    law=dict(effective_date='2020-01-01',known_at=proof['known_at'],standard_units=2000,
        mini_units=None,asset_class='stock_future',source_content_sha256='f'*64,
        rule='same_security_same_direction; unchanged_units_or_explicit_securities_cap_only')
    if problem=='unreviewed_law':law.pop('rule')
    if problem=='future_law':law['effective_date']='2020-03-01'
    bound=bind_equity_position_families(days,intervals,universe,terms,[law])
    assert bound.height==1 and bound['product'][0]=='AAF'
    if problem not in [None,'active_original','finite_same_original','composed_donor_grade']:
        assert bound['position_limit'][0]==2500. and bound['unit'][0]=='contracts'
    else:
        assert bound['position_limit'][0]==6_000_000. and bound['unit'][0]=='shares'
        assert bound['position_unit'][0]==2000.
        assert bound['combined_products'][0].to_list()==['AA1','AAF']
        assert bound['position_binding_status'][0]=='bound_named_securities_position'
        assert set(bound['source_content_sha256s'][0])=={'a'*64,'b'*64,'f'*64}
        assert bound['known_at'][0]=='2020-01-30T15:59:59+00:00'
        assert bound['valid_until_date_exclusive'][0]==(None if problem=='active_original' else '2020-06-18')


def test_reused_adjusted_code_cannot_keep_old_one_for_one_position_units():
    from stockagent.data.tw_futures_margin_preparation import bind_equity_position_families,position_candidate_intervals
    u=pl.DataFrame(dict(product=['AAF','AA1'],product_name=['甲期貨']*2,
        underlying_symbol=['1000']*2,asset_class=['stock_future']*2))
    proof=dict(issue_date_bound=True,source_content_sha256='a'*64,source_url='official',
        known_at='2019-12-20T23:59:59+08:00',effective_date='2020-01-01',
        effective_phase='product_regular_open',unit='contracts')
    rows,_=position_candidate_intervals([
        dict(proof,product='AAF',natural_person_limit=1000),
        dict(proof,product='AA1',event_type='combined_position_formula',
             combined_position_base_product='AAF',combined_position_ratio='1/1',
             extraction_method='explicit_unchanged_unit_corporate_combination')])
    intervals=pl.DataFrame(rows,infer_schema_length=None,schema_overrides={
        'admission_not_before':pl.String,'valid_until_date_exclusive':pl.String,
        'monthly_position_limit':pl.Float64})
    ds=[date(2020,1,2),date(2020,2,3)]
    days=pl.DataFrame(dict(date=ds,product=['AA1']*2))
    terms=days.with_columns(pl.lit('202006').alias('contract'),
        pl.lit('bound_prior_publication').alias('terms_binding_status'),
        pl.Series('contract_multiplier',[2000.,2100.]),
        pl.Series('source_content_sha256s',[['b'*64],['c'*64]]))
    law=dict(effective_date='2020-01-01',known_at=proof['known_at'],
        standard_units=2000,mini_units=None,asset_class='stock_future',source_content_sha256='f'*64)
    bound=bind_equity_position_families(days,intervals,u,terms,[law])
    assert bound['position_numeric_inputs_resolved'].to_list()==[True,False]
    assert bound['position_limit'].to_list()==[1000.,None]
    assert bound['position_binding_status'][1]=='dated_position_units_unresolved'


def test_explicit_share_group_and_inherited_member_keep_root_only_contract_cap_separate():
    from stockagent.data.tw_futures_margin_preparation import bind_equity_position_families,position_candidate_intervals
    products=['AAF','AA1','AA2']; d=date(2020,1,3)
    universe=pl.DataFrame(dict(product=products,product_name=['甲期貨']*3,
        underlying_symbol=['1000']*3,asset_class=['stock_future']*3))
    proof=dict(issue_date_bound=True,source_content_sha256='a'*64,source_url='official',
        known_at='2019-12-20T23:59:59+08:00',effective_date='2020-01-01',
        effective_phase='product_regular_open')
    combined=dict(proof,event_type='corporate_securities_unit_limit',unit='shares',
        natural_person_limit=2500000,combined_products=products)
    rows,issues=position_candidate_intervals([
        dict(combined,product='AAF',position_unit=2000),
        dict(combined,product='AA1',position_unit=2200),
        dict(proof,source_content_sha256='b'*64,product='AAF',event_type='absolute_level',
            unit='contracts',natural_person_limit=350)])
    assert not issues
    intervals=pl.DataFrame(rows,infer_schema_length=None,schema_overrides={
        'admission_not_before':pl.String,'valid_until_date_exclusive':pl.String,
        'combined_position_base_product':pl.String,'monthly_position_limit':pl.Float64})
    days=pl.DataFrame(dict(date=[d]*3,product=products))
    terms=days.with_columns(pl.lit('202003').alias('contract'),
        pl.lit('bound_prior_publication').alias('terms_binding_status'),
        pl.Series('contract_multiplier',[2000.,2200.,2000.]),
        pl.Series('source_content_sha256s',[['c'*64]]*3))
    law=dict(effective_date='2020-01-01',known_at=proof['known_at'],standard_units=2000,
        mini_units=None,asset_class='stock_future',source_content_sha256='f'*64)
    bound=bind_equity_position_families(days,intervals,universe,terms,[law]).sort('product')
    assert bound['position_numeric_inputs_resolved'].all()
    assert bound['position_root_product'].to_list()==['AAF']*3
    assert bound['position_unit'].to_list()==[2200.,2000.,2000.]
    assert bound['independent_contract_limit'].to_list()==[None,None,350.]
    assert bound['position_limit'].to_list()==[2500000.]*3


def test_missing_month_units_neither_disable_nor_borrow_a_verified_peer():
    from stockagent.data.tw_futures_margin_preparation import (
        bind_equity_position_families,bind_physical_position_inputs,position_candidate_intervals)
    u=pl.DataFrame(dict(product=['AAF','AA1'],product_name=['甲期貨']*2,
        underlying_symbol=['1000']*2,asset_class=['stock_future']*2))
    proof=dict(issue_date_bound=True,source_content_sha256='a'*64,source_url='official',
        known_at='2019-12-20T23:59:59+08:00',effective_date='2020-01-01',
        effective_phase='product_regular_open',unit='shares',event_type='corporate_securities_unit_limit',
        combined_products=['AAF','AA1'],position_unit=2000.,natural_person_limit=4000000)
    rows,_=position_candidate_intervals([dict(proof,product='AAF')])
    intervals=pl.DataFrame(rows,infer_schema_length=None,schema_overrides={
        'admission_not_before':pl.String,'valid_until_date_exclusive':pl.String,
        'combined_position_base_product':pl.String})
    days=pl.DataFrame(dict(date=[date(2020,1,2)]*2,product=['AA1']*2,contract=['202003','202006']))
    units=days.with_columns(pl.Series('terms_binding_status',['bound_prior_publication','no_prior_terms']),
        pl.Series('contract_multiplier',[2100.,None]),pl.Series('source_content_sha256s',[['b'*64],None]))
    law=dict(effective_date='2020-01-01',known_at=proof['known_at'],
        standard_units=2000,mini_units=None,asset_class='stock_future',source_content_sha256='f'*64)
    product=bind_equity_position_families(days,intervals,u,units,[law])
    result=bind_physical_position_inputs(days,product,units,u,[law])
    assert result['position_numeric_inputs_resolved'].to_list()==[True,False]
    assert result['position_limit'].to_list()==[4000000.,None]
    assert not result['training_admitted'].any()


@pytest.mark.parametrize('direct_adjusted', [True, False])
def test_reused_adjusted_code_cannot_reactivate_another_months_share_cap(direct_adjusted):
    from stockagent.data.tw_futures_margin_preparation import (
        bind_equity_position_families,bind_physical_position_inputs,position_candidate_intervals)
    universe=pl.DataFrame(dict(product=['AAF','AA1'],product_name=['甲期貨']*2,
        underlying_symbol=['1000']*2,asset_class=['stock_future']*2))
    proof=dict(issue_date_bound=True,source_content_sha256='a'*64,source_url='old-original',
        known_at='2019-12-20T23:59:59+08:00',effective_date='2020-01-01',
        effective_phase='product_regular_open',unit='shares',event_type='corporate_securities_unit_limit',
        combined_products=['AAF','AA1'],natural_person_limit=4000000)
    facts=[dict(proof,product='AAF',position_unit=2000.)]
    if direct_adjusted:facts.append(dict(proof,product='AA1',position_unit=2100.))
    rows,_=position_candidate_intervals(facts)
    intervals=pl.DataFrame(rows,infer_schema_length=None,schema_overrides={
        'admission_not_before':pl.String,'valid_until_date_exclusive':pl.String,
        'combined_position_base_product':pl.String})
    days=pl.DataFrame(dict(date=[date(2020,1,2)]*3,product=['AA1','AA1','AAF'],
        contract=['202003','202006','202006']))
    units=days.with_columns(pl.lit('bound_prior_publication').alias('terms_binding_status'),
        pl.Series('contract_multiplier',[2100.,2100.,2000.]),
        pl.Series('source_content_sha256s',[['a'*64],['b'*64],['b'*64]]))
    law=dict(effective_date='2020-01-01',known_at=proof['known_at'],standard_units=2000,
        mini_units=None,asset_class='stock_future',source_content_sha256='f'*64)
    product=bind_equity_position_families(days,intervals,universe,units,[law])
    scoped=pl.DataFrame(dict(product=['AA1','AA1'],contract=['202003','202006'],
        source_content_sha256s=[['a'*64],['b'*64]]))
    result=bind_physical_position_inputs(days,product,units,universe,[law],
        corporate_unit_intervals=scoped).sort('product','contract')
    adjusted=result.filter(pl.col('product')=='AA1')
    assert adjusted['position_numeric_inputs_resolved'].to_list()==[True,not direct_adjusted]
    assert adjusted['position_limit'].to_list()==[4000000.,None if direct_adjusted else 4000000.]
    if direct_adjusted:
        assert adjusted['position_binding_status'][1]=='physical_position_month_scope_unresolved'
    assert result.filter(pl.col('product')=='AAF')['position_limit'].to_list()==[4000000.]
    assert not result['training_admitted'].any()


@pytest.mark.parametrize('problem', [None, 'declared_old', 'changed_units', 'missing_unit',
    'future_unit_clock', 'missing_clock', 'no_base', 'law_not_known', 'no_law', 'missing_source_hash'])
def test_new_physical_generation_uses_only_verified_unchanged_unit_grouping(problem):
    from datetime import datetime
    from stockagent.data.tw_futures_margin_preparation import (
        bind_equity_position_families, bind_physical_position_inputs, position_candidate_intervals)
    universe=pl.DataFrame(dict(product=['AAF','AA1'],product_name=['甲期貨']*2,
        underlying_symbol=['1000']*2,asset_class=['stock_future']*2))
    old=dict(product='AA1',issue_date_bound=True,source_content_sha256='a'*64,
        source_url='old-adjustment',known_at='2023-01-02T23:59:59+08:00',effective_date='2023-01-03',
        effective_phase='product_regular_open',unit='shares',event_type='corporate_securities_unit_limit',
        combined_products=['AAF','AA1'],position_unit=1271.522,natural_person_limit=4000000)
    base=dict(product='AAF',issue_date_bound=True,source_content_sha256='c'*64,
        source_url='new-standard-cap',known_at='2023-01-25T23:59:59+08:00',effective_date='2023-02-01',
        effective_phase='product_regular_open',unit='contracts',event_type='absolute_level',natural_person_limit=2000)
    rows,_=position_candidate_intervals([old]+([] if problem=='no_base' else [base]))
    intervals=pl.DataFrame(rows,infer_schema_length=None,schema_overrides={
        'admission_not_before':pl.String,'valid_until_date_exclusive':pl.String,
        'combined_position_base_product':pl.String})
    days=pl.DataFrame(dict(date=[date(2023,2,15)]*2,product=['AA1','AAF'],contract=['202306']*2))
    unit=1271.522 if problem=='declared_old' else 2100. if problem=='changed_units' else None if problem=='missing_unit' else 2000.
    known=None if problem=='missing_clock' else '2023-02-16T23:59:59+08:00' if problem=='future_unit_clock' else '2023-02-06T23:59:59+08:00'
    own_hash='a'*64 if problem=='declared_old' else 'b'*64
    units=days.with_columns(pl.Series('terms_binding_status',[
        'no_prior_terms' if problem=='missing_unit' else 'bound_prior_publication','bound_prior_publication']),
        pl.Series('contract_multiplier',[unit,2000.]),
        pl.Series('known_at',[known,'2023-01-25T23:59:59+08:00']),
        pl.Series('source_content_sha256s',[[] if problem=='missing_source_hash' else [own_hash],['c'*64]]))
    law=dict(effective_date='2020-01-01',known_at='2023-02-15T23:59:59+08:00' if problem=='law_not_known'
        else '2019-12-20T23:59:59+08:00',standard_units=2000,mini_units=None,
        asset_class='stock_future',source_content_sha256='f'*64)
    laws=[] if problem=='no_law' else [law]
    product=bind_equity_position_families(days,intervals,universe,units,laws)
    declarations=pl.DataFrame(dict(product=['AA1','AA1'],contract=['202303','202306'],
        source_content_sha256s=[['a'*64],[own_hash]]))
    result=bind_physical_position_inputs(days,product,units,universe,laws,
        corporate_unit_intervals=declarations)
    row=result.filter(pl.col('product')=='AA1').row(0,named=True)
    assert row['position_numeric_inputs_resolved'] is (problem in (None,'declared_old'))
    if problem is None:
        assert row['position_binding_status']=='bound_same_security_position'
        assert row['unit']=='contracts' and row['position_unit']==1. and row['position_limit']==2000.
        assert datetime.fromisoformat(row['known_at'])==datetime.fromisoformat('2023-02-06T23:59:59+08:00')
        assert row['source_content_sha256s']==['b'*64,'c'*64,'f'*64]
        assert row['position_interval_id'] is None and row['position_base_interval_id'] is not None
        assert row['position_corporate_month_scope_bound']
    elif problem=='declared_old':
        assert row['unit']=='shares' and row['position_unit']==1271.522 and row['position_limit']==4000000.
        assert row['source_content_sha256s']==['a'*64]
    else:
        assert row['position_limit'] is None and row['position_binding_status']!='bound_same_security_position'
    assert not result['training_admitted'].any()


@pytest.mark.parametrize('problem', [None, 'old_month_reused', 'no_proof', 'wrong_source',
    'wrong_month', 'wrong_base', 'proof_not_effective', 'proof_ended', 'proof_same_day',
    'changed_units', 'unit_same_day', 'missing_unit_clock', 'law_same_day', 'no_law',
    'unresolved_base', 'own_independent', 'base_independent', 'ended_clock', 'own_numeric_cap'])
def test_reused_generation_share_cap_requires_own_positive_member_proof(problem):
    from stockagent.data.tw_futures_margin_preparation import (
        bind_equity_position_families,bind_physical_position_inputs,position_candidate_intervals)
    universe=pl.DataFrame(dict(product=['AAF','AA1'],product_name=['甲期貨']*2,
        underlying_symbol=['1000']*2,asset_class=['stock_future']*2))
    common=dict(issue_date_bound=True,known_at='2023-01-02T23:59:59+08:00',
        effective_date='2023-01-03',effective_phase='product_regular_open',unit='shares',
        event_type='corporate_securities_unit_limit',combined_products=['AAF','AA1'])
    own_numeric=problem=='own_numeric_cap'
    facts=[dict(common,product='AA1',source_content_sha256=('b' if own_numeric else 'a')*64,
        source_url='old-original',position_unit=2000. if own_numeric else 2100.,
        natural_person_limit=4500000),dict(common,product='AAF',source_content_sha256='c'*64,
        source_url='standard-original',position_unit=2000.,natural_person_limit=4000000)]
    rows,_=position_candidate_intervals(facts)
    intervals=pl.DataFrame(rows,infer_schema_length=None,schema_overrides={
        'admission_not_before':pl.String,'valid_until_date_exclusive':pl.String,
        'combined_position_base_product':pl.String})
    day=date(2023,2,15)
    days=pl.DataFrame(dict(date=[day]*2,product=['AA1','AAF'],contract=['202306']*2))
    units=days.with_columns(pl.lit('bound_prior_publication').alias('terms_binding_status'),
        pl.Series('contract_multiplier',[2100. if problem=='changed_units' else 2000.,2000.]),
        pl.Series('known_at',[None if problem=='missing_unit_clock' else
            '2023-02-15T01:00:00+08:00' if problem=='unit_same_day' else '2023-02-06T23:59:59+08:00',
            '2023-01-02T23:59:59+08:00']),
        pl.Series('source_content_sha256s',[['b'*64],['c'*64]]))
    law=dict(effective_date='2020-01-01',known_at='2023-02-15T01:00:00+08:00'
        if problem=='law_same_day' else '2019-12-20T23:59:59+08:00',standard_units=2000,
        mini_units=None,asset_class='stock_future',source_content_sha256='f'*64)
    laws=[] if problem=='no_law' else [law]
    product=bind_equity_position_families(days,intervals,universe,units,laws)
    if problem in ('own_independent','base_independent'):
        product=product.with_columns(pl.when(pl.col('product')==
            ('AA1' if problem=='own_independent' else 'AAF')).then(1000.)
            .otherwise(pl.col('independent_contract_limit')).alias('independent_contract_limit'))
    if problem=='ended_clock':
        product=product.with_columns(pl.when(pl.col('product')=='AA1').then(pl.lit('interval_ended'))
            .otherwise(pl.col('position_binding_status')).alias('position_binding_status'))
    if problem=='unresolved_base':
        product=product.with_columns(pl.when(pl.col('product')=='AAF').then(False)
            .otherwise(pl.col('position_numeric_inputs_resolved')).alias('position_numeric_inputs_resolved'))
    declarations=pl.DataFrame(dict(product=['AA1','AA1'],
        contract=['202306' if problem=='old_month_reused' else '202303','202306'],
        source_content_sha256s=[['a'*64],['b'*64]]))
    proof=pl.DataFrame([dict(product='AA1',contract='202309' if problem=='wrong_month' else '202306',
        standard_product='ZZF' if problem=='wrong_base' else 'AAF',
        source_content_sha256=('z' if problem=='wrong_source' else 'b')*64,
        visual_review_sha256='d'*64,effective_date='2023-02-16' if problem=='proof_not_effective' else '2023-02-07',
        known_at='2023-02-15T01:00:00+08:00' if problem=='proof_same_day' else '2023-02-06T23:59:59+08:00',
        valid_until_date_exclusive='2023-02-15' if problem=='proof_ended' else None,contract_multiplier=2000.)],
        schema_overrides={'valid_until_date_exclusive':pl.String})
    if problem=='wrong_source':proof=proof.with_columns(pl.lit('e'*64).alias('source_content_sha256'))
    result=bind_physical_position_inputs(days,product,units,universe,laws,
        corporate_unit_intervals=declarations,unchanged_member_scopes=None if problem=='no_proof' else proof)
    row=result.filter(pl.col('product')=='AA1').row(0,named=True)
    assert row['position_numeric_inputs_resolved'] is (problem in (None,'old_month_reused','own_numeric_cap'))
    if problem in (None,'old_month_reused'):
        assert row['position_binding_status']=='bound_same_security_position'
        assert row['unit']=='shares' and row['position_unit']==2000. and row['position_limit']==4000000.
        assert row['source_content_sha256s']==['b'*64,'c'*64,'f'*64]
        assert row['position_corporate_month_scope_bound'] and row['position_base_interval_id'] is not None
    elif own_numeric:
        assert row['position_limit']==4500000. and row['source_content_sha256s']==['b'*64]
    else:assert row['position_limit'] is None
    assert not result['training_admitted'].any()


def test_unchanged_member_scopes_bind_own_formula_and_exact_months():
    from stockagent.data.tw_futures_margin_preparation import unchanged_position_member_scopes
    fact=dict(product='AA1',combined_position_base_product='AAF',
        extraction_method='source_bound_visual_corporate_position_group',
        position_formula_parser='explicit_unchanged_unit_corporate_combination',
        event_type='combined_position_formula',combined_position_ratio='1/1',
        natural_person_limit=None,issue_date_bound=True,source_content_sha256='b'*64,
        visual_review_sha256='c'*64,effective_date='2023-02-07',known_at='2023-02-06T23:59:59+08:00')
    units=pl.DataFrame([dict(product='AA1',contract='202303',source_content_sha256s=['b'*64],
        effective_date=fact['effective_date'],known_at=fact['known_at'],contract_multiplier=2000.,
        valid_until_date_exclusive=None),dict(product='AA1',contract='202306',source_content_sha256s=['a'*64],
        effective_date='2023-01-03',known_at='2023-01-02T23:59:59+08:00',contract_multiplier=2100.,
        valid_until_date_exclusive=None)],schema_overrides={'valid_until_date_exclusive':pl.String})
    result=unchanged_position_member_scopes([fact],units)
    assert result['contract'].to_list()==['202303'] and result['source_content_sha256'].to_list()==['b'*64]
    for change in [dict(combined_position_ratio='2/1'),dict(visual_review_sha256=None),
            dict(source_content_sha256='a'*64),dict(known_at='2023-02-05T23:59:59+08:00')]:
        with pytest.raises(ValueError):unchanged_position_member_scopes([dict(fact,**change)],units)
    with pytest.raises(ValueError):
        unchanged_position_member_scopes([fact],units.with_columns(pl.lit(2100.).alias('contract_multiplier')))


@pytest.mark.parametrize('problem', [None,'corrupt_source','missing_original','missing_review','changed_scope','changed_units'])
def test_unchanged_member_bundle_rechecks_original_review_and_derived_scope(tmp_path,problem):
    import gzip
    from downloader.artifact_io import sha256_file
    from stockagent.data.tw_futures_margin_preparation import (
        unchanged_position_member_scopes,load_unchanged_position_member_scopes)
    raw=b'own original bytes';original=tmp_path/'original.body.gz';original.write_bytes(gzip.compress(raw))
    review=tmp_path/'review.json';review.write_text('{"inspected":true}')
    fact=dict(product='AA1',combined_position_base_product='AAF',
        extraction_method='source_bound_visual_corporate_position_group',
        position_formula_parser='explicit_unchanged_unit_corporate_combination',
        event_type='combined_position_formula',combined_position_ratio='1/1',natural_person_limit=None,
        issue_date_bound=True,source_content_sha256=hashlib.sha256(raw).hexdigest(),
        visual_review_sha256=sha256_file(review),effective_date='2023-02-07',known_at='2023-02-06T23:59:59+08:00')
    units=pl.DataFrame([dict(product='AA1',contract='202303',source_content_sha256s=[fact['source_content_sha256']],
        effective_date=fact['effective_date'],known_at=fact['known_at'],contract_multiplier=2000.,
        valid_until_date_exclusive=None)],schema_overrides={'valid_until_date_exclusive':pl.String})
    scope=unchanged_position_member_scopes([fact],units)
    if problem=='changed_scope':scope=scope.with_columns(pl.lit('202306').alias('contract'))
    if problem=='changed_units':units=units.with_columns(pl.lit(2100.).alias('contract_multiplier'))
    paths=[tmp_path/'unchanged_member_scopes.parquet',tmp_path/'source_bound_unit_intervals.parquet',
        tmp_path/'reparsed_member_formulas.json']
    scope.write_parquet(paths[0]);units.write_parquet(paths[1]);paths[2].write_text(json.dumps([fact]))
    sources=[dict(path=original.name,sha256=sha256_file(original),kind='raw_gzip'),
        dict(path=review.name,sha256=sha256_file(review),kind='visual_position_cell_review')]
    if problem=='missing_original':sources=sources[1:]
    if problem=='missing_review':sources=sources[:1]
    manifest=dict(schema_version=1,status='source_verified_unchanged_physical_member_scopes',
        numeric_position_caps_added=0,sources=sources,
        outputs={p.name:dict(sha256=sha256_file(p)) for p in paths})
    (tmp_path/'manifest.json').write_text(json.dumps(manifest))
    if problem=='corrupt_source':original.write_bytes(b'changed')
    if problem:
        with pytest.raises(ValueError):load_unchanged_position_member_scopes(paths[0])
    else:
        actual,proofs=load_unchanged_position_member_scopes(paths[0])
        assert actual.equals(scope) and str(original) in proofs and str(review) in proofs


def incumbent_position_fixture():
    fact=dict(product='AA1',source_content_sha256='a'*64,visual_review_sha256='c'*64,
        source_url='printed-cap',published_date='2020-01-20',known_at='2020-01-20T23:59:59+08:00',
        issue_date_bound=True,position_unit_review_scope='all_named_members_in_inspected_grid',
        extraction='source_bound_visual_corporate_cells',unit='shares',event_type='corporate_securities_unit_limit',
        effective_date='2020-02-03',effective_phase='product_regular_open',valid_until_date_inclusive='2020-03-18',
        position_unit=2162.,natural_person_limit=25806400.,combined_products=['AAF','AA1','AA2'])
    declarations=pl.DataFrame([dict(product='AA1',contract='202003',contract_multiplier=2162.,
        effective_date='2020-01-02',known_at='2019-12-20T23:59:59+08:00',
        source_content_sha256s=['b'*64],valid_until_exclusive=None)],
        schema_overrides={'valid_until_exclusive':pl.String})
    return fact,declarations


@pytest.mark.parametrize('problem',[None,'later_same_units','same_day_terms','same_day_effective',
    'wrong_quantity','wrong_member','wrong_base','no_numeric_cap','no_review','naive_clock','expired_terms'])
def test_reviewed_incumbent_grid_binds_only_previously_declared_own_months(problem):
    from stockagent.data.tw_futures_margin_preparation import reviewed_incumbent_position_member_scopes
    fact,units=incumbent_position_fixture()
    changes={'later_same_units':('known_at','2020-03-01T23:59:59+08:00'),
        'same_day_terms':('known_at',fact['known_at']),
        'same_day_effective':('effective_date',fact['published_date']),
        'wrong_quantity':('contract_multiplier',2100.),'expired_terms':('valid_until_exclusive','2020-02-03')}
    if problem in changes:
        key,value=changes[problem];units=units.with_columns(pl.lit(value).alias(key))
    if problem in ['wrong_member','wrong_base']:
        fact['combined_products']=['AAF','AA2'] if problem=='wrong_member' else ['AA1','AA2']
    if problem=='no_numeric_cap':fact['natural_person_limit']=None
    if problem=='no_review':fact['visual_review_sha256']=None
    if problem=='naive_clock':fact['known_at']='2020-01-20T23:59:59'
    if problem in ['wrong_member','wrong_base','no_numeric_cap','no_review','naive_clock']:
        with pytest.raises(ValueError):reviewed_incumbent_position_member_scopes([fact],units)
        return
    scopes=reviewed_incumbent_position_member_scopes([fact],units)
    if problem:assert scopes.is_empty()
    else:
        assert scopes['contract'].to_list()==['202003']
        assert scopes['position_source_sha256'].to_list()==['a'*64]
        assert scopes['unit_source_sha256'].to_list()==['b'*64]
        assert scopes['contract_multiplier'].to_list()==[2162.]
        assert scopes['valid_until_date_exclusive'].to_list()==['2020-03-19']


@pytest.mark.parametrize('problem',[None,'no_proof','wrong_month','later_generation_same_quantity',
    'different_quantity','different_cap_original','different_unit_clock','before_cap','ended_proof',
    'unresolved_terms','unresolved_cap','missing_law','missing_group_member'])
def test_incumbent_physical_binding_preserves_numeric_clock_and_generation_checks(problem):
    from stockagent.data.tw_futures_margin_preparation import (reviewed_incumbent_position_member_scopes,
        bind_equity_position_families,bind_physical_position_inputs,position_candidate_intervals)
    fact,declarations=incumbent_position_fixture()
    scopes=reviewed_incumbent_position_member_scopes([fact],declarations)
    facts=[fact,dict(fact,product='AAF',position_unit=2000.)]
    if problem=='different_cap_original':facts=[dict(f,source_content_sha256='d'*64) for f in facts]
    rows,_=position_candidate_intervals(facts)
    intervals=pl.DataFrame(rows,infer_schema_length=None,schema_overrides={
        'admission_not_before':pl.String,'valid_until_date_exclusive':pl.String,
        'combined_position_base_product':pl.String})
    day=date(2020,2,4) if problem!='before_cap' else date(2020,1,31)
    days=pl.DataFrame(dict(date=[day]*2,product=['AA1','AAF'],contract=['202003']*2))
    units=days.with_columns(pl.lit('bound_prior_publication').alias('terms_binding_status'),
        pl.Series('contract_multiplier',[2100. if problem=='different_quantity' else 2162.,2000.]),
        pl.Series('known_at',['2019-12-21T23:59:59+08:00' if problem=='different_unit_clock'
            else '2019-12-20T23:59:59+08:00']*2),
        pl.Series('source_content_sha256s',[['e'*64 if problem=='later_generation_same_quantity' else 'b'*64],['f'*64]]))
    if problem=='unresolved_terms':units=units.with_columns(pl.lit('no_prior_terms').alias('terms_binding_status'))
    universe=pl.DataFrame(dict(product=['AAF','AA1'],product_name=['甲期貨']*2,
        underlying_symbol=['1000']*2,asset_class=['stock_future']*2))
    law=dict(effective_date='2010-01-01',known_at='2009-12-20T23:59:59+08:00',standard_units=2000,
        mini_units=None,asset_class='stock_future',source_content_sha256='f'*64)
    laws=[] if problem=='missing_law' else [law]
    product=bind_equity_position_families(days,intervals,universe,units,laws)
    if problem=='unresolved_cap':product=product.with_columns(pl.lit(False).alias('position_numeric_inputs_resolved'))
    if problem=='missing_group_member':product=product.with_columns(pl.lit(['AAF','AA2']).alias('combined_products'))
    if problem=='wrong_month':scopes=scopes.with_columns(pl.lit('202006').alias('contract'))
    if problem=='ended_proof':scopes=scopes.with_columns(pl.lit('2020-02-04').alias('valid_until_date_exclusive'))
    result=bind_physical_position_inputs(days,product,units,universe,laws,
        corporate_unit_intervals=declarations,
        reviewed_incumbent_member_scopes=None if problem=='no_proof' else scopes)
    row=result.filter(pl.col('product')=='AA1').row(0,named=True)
    assert row['position_numeric_inputs_resolved'] is (problem is None)
    if problem is None:
        assert row['position_limit']==25806400. and row['position_unit']==2162.
        assert row['position_corporate_month_scope_bound']
        assert row['source_content_sha256s']==['a'*64]
    else:assert row['position_limit'] is None
    assert not result['training_admitted'].any()


@pytest.mark.parametrize('problem',[None,'changed_scope','changed_units','missing_cap_original',
    'missing_own_original','missing_review','corrupt_source'])
def test_incumbent_scope_bundle_verifies_both_originals_and_exact_derived_months(tmp_path,problem):
    import gzip
    from downloader.artifact_io import sha256_file
    from stockagent.data.tw_futures_margin_preparation import (
        reviewed_incumbent_position_member_scopes,load_reviewed_incumbent_position_member_scopes)
    cap=tmp_path/'cap.gz';cap.write_bytes(gzip.compress(b'printed incumbent grid'))
    own=tmp_path/'own.gz';own.write_bytes(gzip.compress(b'prior own monthly declaration'))
    review=tmp_path/'review.json';review.write_text('{"source_verified":true}')
    fact,units=incumbent_position_fixture()
    fact.update(source_content_sha256=hashlib.sha256(gzip.decompress(cap.read_bytes())).hexdigest(),
        visual_review_sha256=sha256_file(review))
    units=units.with_columns(pl.lit([hashlib.sha256(gzip.decompress(own.read_bytes())).hexdigest()]).alias('source_content_sha256s'))
    scopes=reviewed_incumbent_position_member_scopes([fact],units)
    if problem=='changed_scope':scopes=scopes.with_columns(pl.lit('202006').alias('contract'))
    if problem=='changed_units':units=units.with_columns(pl.lit(2100.).alias('contract_multiplier'))
    paths=[tmp_path/'incumbent_member_scopes.parquet',tmp_path/'source_bound_unit_intervals.parquet',
        tmp_path/'reparsed_incumbent_position_facts.json']
    scopes.write_parquet(paths[0]);units.write_parquet(paths[1]);paths[2].write_text(json.dumps([fact]))
    sources=[dict(path=p.name,sha256=sha256_file(p),kind='raw_gzip' if p!=review else 'visual_position_cell_review')
        for p in [cap,own,review]]
    if problem=='missing_cap_original':sources=sources[1:]
    if problem=='missing_own_original':sources=[s for s in sources if s['path']!=own.name]
    if problem=='missing_review':sources=sources[:2]
    (tmp_path/'manifest.json').write_text(json.dumps(dict(schema_version=1,
        status='source_verified_incumbent_physical_member_scopes',numeric_position_caps_added=0,
        financial_events_added=0,sources=sources,outputs={p.name:dict(sha256=sha256_file(p)) for p in paths})))
    if problem=='corrupt_source':own.write_bytes(b'corrupt')
    if problem:
        with pytest.raises(ValueError):load_reviewed_incumbent_position_member_scopes(paths[0])
    else:
        actual,proofs=load_reviewed_incumbent_position_member_scopes(paths[0])
        assert actual.equals(scopes) and str(own) in proofs and str(cap) in proofs


def interval(**changes):
    row = dict(product='TX', effective_date='2020-01-01', effective_phase=0,
        known_at='2019-12-31T23:59:59+08:00', valid_until_date_exclusive=None,
        valid_until_phase_exclusive=None, margin_kind='fixed_twd', initial=100.,
        maintenance=80., source_content_sha256s=['a'*64])
    row.update(changes)
    return row


def bind(rows, days=('2020-01-02',), products=('TX',)):
    events = pl.DataFrame(rows, schema_overrides={
        'valid_until_date_exclusive': pl.String, 'valid_until_phase_exclusive': pl.Int64})
    queries = pl.DataFrame([dict(date=date.fromisoformat(day), product=product)
                           for day in days for product in products])
    return align_product_margin_intervals(queries, events)


def test_after_close_margin_changes_only_settlement_phase():
    result = bind([interval(valid_until_date_exclusive='2020-01-02', valid_until_phase_exclusive=1),
        interval(effective_date='2020-01-02', effective_phase=1, initial=120.)]).row(0, named=True)
    assert result['opening_initial'] == 100.
    assert result['settlement_initial'] == 120.


def test_unresolved_restoration_leaves_a_gap_until_next_absolute_level():
    result = bind([interval(valid_until_date_exclusive='2020-01-02', valid_until_phase_exclusive=1),
        interval(effective_date='2020-01-04', initial=120.)],
        days=('2020-01-02', '2020-01-03', '2020-01-04'))
    assert result['opening_initial'].to_list() == [100., None, 120.]
    assert result['settlement_initial'].to_list() == [None, None, 120.]
    assert result['settlement_binding_status'][1] == 'interval_ended'


def test_not_yet_public_replacement_does_not_revive_old_margin():
    result = bind([interval(valid_until_date_exclusive='2020-01-02', valid_until_phase_exclusive=0),
        interval(effective_date='2020-01-02', known_at='2020-01-03T01:00:00+08:00', initial=120.)],
        days=('2020-01-02', '2020-01-03', '2020-01-04'))
    assert result['opening_initial'].to_list() == [None, None, 120.]
    assert result['opening_binding_status'].to_list() == [
        'not_yet_public', 'same_day_clock_review', 'bound_prior_publication']


def test_taipei_publication_day_and_explicit_product_scope():
    result = bind([interval(known_at='2020-01-01T17:00:00Z')], products=('TX', 'TX1'))
    assert result['opening_binding_status'].to_list() == ['same_day_clock_review', 'no_prior_interval']
    assert result['opening_initial'].null_count() == 2


def test_future_event_cannot_change_past_bound_amounts():
    old = interval(valid_until_date_exclusive='2020-02-01', valid_until_phase_exclusive=0)
    first = bind([old, interval(effective_date='2020-02-01', initial=120.)])
    perturbed = bind([old, interval(effective_date='2020-02-01', initial=10000.)])
    assert first.equals(perturbed)


@pytest.mark.parametrize('rows,message', [
    ([interval(), interval()], 'duplicate'),
    ([interval(), interval(effective_date='2020-01-02')], 'overlapping'),
    ([interval(initial=0)], 'amounts'),
    ([interval(margin_kind='notional_rate')], 'amounts'),
    ([interval(source_content_sha256s=[])], 'source identities'),
    ([interval(known_at='2020-01-01T00:00:00')], 'timezone'),
    ([interval(valid_until_date_exclusive='2020-01-03')], 'explicit phase'),
])
def test_invalid_or_ambiguous_inputs_are_rejected(rows, message):
    with pytest.raises(ValueError, match=message):
        bind(rows)


@pytest.mark.parametrize('alter', [None, 'amount', 'source', 'image'])
def test_visual_product_correction_is_source_and_amount_bound(tmp_path, alter):
    from scripts.build_tw_futures_margin_event_candidates import apply_margin_code_reviews
    image = tmp_path / 'page.png'
    image.write_bytes(b'reviewed-page-fixture')
    cell = dict(source_url='https://www.taifex.com.tw/source.pdf', content_sha256='a'*64,
        page=4, page_image=image.name, page_image_sha256=hashlib.sha256(image.read_bytes()).hexdigest(),
        ocr_product='GIF', product='GTF', margin_kind='fixed_twd',
        after=[26000., 20000., 19000.], before=[23000., 18000., 17000.])
    path = tmp_path / 'review.json'
    path.write_text(json.dumps(dict(review_kind='source_bound_margin_product_code_cells', cells=[cell])))
    fact = dict(source_url=cell['source_url'], source_content_sha256='a'*64,
        product='GIF', margin_kind='fixed_twd', after=cell['after'], before=cell['before'])
    genuine = dict(fact, source_content_sha256='b'*64, margin_kind='notional_rate',
                   after=[.135, .1035, .1], before=None)
    class Archive:
        def document(self, url):
            return {'content_sha256': 'c'*64 if alter == 'source' else 'a'*64}
        def copy(self, *args, **kwargs):
            pass
    if alter == 'amount':
        fact['after'] = [27000., 20000., 19000.]
    if alter == 'image':
        image.write_bytes(b'changed-page')
    if alter:
        with pytest.raises(ValueError):
            apply_margin_code_reviews(Archive(), [fact, genuine], path)
    else:
        result = apply_margin_code_reviews(Archive(), [fact, genuine], path)
        assert [row['product'] for row in result] == ['GTF', 'GIF']
        assert fact['product'] == 'GIF'  # original candidate is preserved
        assert result[0]['source_product_code'] == 'GIF'
        assert result[1] == genuine


def generation_position_review_fixture(tmp_path):
    from types import SimpleNamespace
    from downloader.artifact_io import sha256_file
    old_image = tmp_path / 'old.png'
    old_image.write_bytes(b'inspected prior group/cap page fixture')
    review_path = tmp_path / 'review.json'
    review_path.write_text('{}')
    old_text = '發文字號：台期交字第1130201223號。'
    new_text = ('註3：113年6月24日台期交字第1130201223號函公告。'
                'LVF調整為LV1。LV1調整為LV2。')
    publications = {'old': '2024-06-25', 'new': '2024-08-28'}
    documents = {'old': dict(content_sha256='a' * 64, text=old_text),
                 'new': dict(content_sha256='b' * 64, text=new_text)}
    class Archive:
        conn = SimpleNamespace(execute=lambda query, args: SimpleNamespace(
            fetchone=lambda: {'published_date': publications[args[0]]}))
        def document(self, url): return documents[url]
        def copy(self, path, digest, **kwargs): assert sha256_file(path) == digest
    common = dict(effective_date='2024-09-19', effective_phase='product_regular_open',
        event_type='corporate_securities_unit_limit', unit='shares',
        natural_person_limit=4000000., natural_person_monthly_limit=None,
        requires_delisting_clock=True, end_rule='until_adjusted_future_delisting_prior_business_day',
        issue_date_bound=True, limit_follows_applicable_grade=True)
    old = [dict(common, product=product, position_unit=units,
        combined_products=['LVF', 'LV1'], source_content_sha256='a' * 64,
        source_url='old', published_date='2024-06-25', known_at='2024-06-25T23:59:59+08:00')
        for product, units in [('LVF', 2000.), ('LV1', 2040.)]]
    new = [dict(common, product=product, position_unit=units,
        combined_products=['LVF', 'LV1', 'LV2'])
        for product, units in [('LVF', 2000.), ('LV1', 2000.), ('LV2', 2040.)]]
    corporate = [dict(product=product, from_product=origin, contract_multiplier=units,
        deliverable_security_quantity=units, contract_months=['202412', '202503'],
        effective_date='2024-09-05', known_at='2024-08-28T23:59:59+08:00',
        issue_date_bound=True, source_content_sha256='b' * 64, source_url='new')
        for origin, product, units in [('LVF', 'LV1', 2000.), ('LV1', 'LV2', 2040.)]]
    review = dict(source_url='new', content_sha256='b' * 64,
        published_date='2024-08-28', effective_date='2024-09-05',
        prior_generation_constraints=[dict(source_url='old', content_sha256='a' * 64,
            published_date='2024-06-25', notice_number='1130201223', effective_date='2024-09-19',
            transfers={'LVF': 'LV1', 'LV1': 'LV2'},
            pages=[dict(page=2, path=old_image.name, sha256=sha256_file(old_image))])])
    return Archive(), review, new, corporate, old, review_path, documents, publications


def test_future_cap_uses_explicit_generation_transfers_and_implied_prior_constraint(tmp_path):
    from copy import deepcopy
    import scripts.build_tw_futures_margin_event_candidates as builder
    from stockagent.data.tw_futures_margin_preparation import position_candidate_intervals
    a, review, new, corporate, old, path, _, _ = generation_position_review_fixture(tmp_path)
    original = deepcopy((old, corporate))
    builder.bind_position_generation_constraints(a, review, new, corporate, old, path)
    assert (old, corporate) == original
    assert all(r['supersedes_source_sha256s'] == ['a' * 64] for r in new)
    proof = json.loads(new[0]['position_generation_constraint_evidence'])
    assert proof['transferred_prior_members'] == ['LV1', 'LV2']
    assert proof['current_combined_members'] == ['LV1', 'LV2', 'LVF']
    common = dict(source_content_sha256='b' * 64, source_url='new', issue_date_bound=True,
        known_at='2024-08-28T23:59:59+08:00', published_date='2024-08-28')
    intervals, issues = position_candidate_intervals([*old, *[dict(r, **common) for r in new]])
    assert not issues
    assert {r['product']: r['position_unit'] for r in intervals} == {
        'LVF': 2000., 'LV1': 2000., 'LV2': 2040.}
    assert len(old) == 2  # Originals remain available; only the declared future boundary changes.


@pytest.mark.parametrize('problem', [
    'unreferenced', 'wrong_old_source', 'late_publication', 'incomplete_transfers',
    'ambiguous_units', 'increased_cap', 'missing_group_member', 'wrong_future_day',
    'unsafe_image', 'image_mismatch', 'late_corporate_clock',
])
def test_future_generation_constraint_rejects_unproven_implication(tmp_path, problem):
    import scripts.build_tw_futures_margin_event_candidates as builder
    a, review, new, corporate, old, path, documents, publications = generation_position_review_fixture(tmp_path)
    proof = review['prior_generation_constraints'][0]
    if problem == 'unreferenced': documents['new']['text'] = 'LVF調整為LV1。LV1調整為LV2。'
    if problem == 'wrong_old_source': proof['content_sha256'] = 'c' * 64
    if problem == 'late_publication':
        review['published_date'] = publications['new'] = '2024-09-19'
    if problem == 'incomplete_transfers': proof['transfers'].pop('LV1')
    if problem == 'ambiguous_units':
        corporate.append(dict(corporate[1], contract_multiplier=2100., deliverable_security_quantity=2100.))
    if problem == 'increased_cap':
        for row in new: row['natural_person_limit'] = 4000001.
    if problem == 'missing_group_member':
        for row in new: row['combined_products'] = ['LVF', 'LV1']
    if problem == 'wrong_future_day': proof['effective_date'] = '2024-09-20'
    if problem == 'unsafe_image': proof['pages'][0]['path'] = '../old.png'
    if problem == 'image_mismatch': (tmp_path / 'old.png').write_bytes(b'altered fixture')
    if problem == 'late_corporate_clock': corporate[0]['known_at'] = '2024-09-05T23:59:59+08:00'
    with pytest.raises(ValueError):
        builder.bind_position_generation_constraints(a, review, new, corporate, old, path)


def complete_original_generation_fixture(tmp_path, monkeypatch):
    """Two complete inspected originals without a cross-notice reference."""
    from copy import deepcopy
    from downloader.artifact_io import sha256_file
    import scripts.build_tw_futures_margin_event_candidates as builder
    a, review, new, corporate, old, path, documents, _ = generation_position_review_fixture(tmp_path)
    documents['old']['text'] = documents['new']['text'] = ''
    for row in old + new:
        row['limit_follows_applicable_grade'] = False
    for row in corporate:
        row['deliverable_components_resolved'] = True
    prior_financial = [dict(corporate[1], from_product='LVF', product='LV1',
        effective_date='2024-06-27', source_url='old', source_content_sha256='a' * 64,
        known_at='2024-06-25T23:59:59+08:00')]
    originals = {'old': (deepcopy(prior_financial), deepcopy(old)),
                 'new': (deepcopy(corporate), deepcopy(new))}
    proof = review['prior_generation_constraints'][0]
    proof['proof_kind'] = 'verified_originals_unchanged_transfer'
    for role, url in [('prior', 'old'), ('current', 'new')]:
        source = tmp_path / (role + '.json')
        source.write_text(json.dumps({'owned_original': url}))
        proof[role + '_source_review'] = dict(path=str(source), sha256=sha256_file(source))
    def reader(archive, source, *, source_urls):
        assert archive is a and len(source_urls) == 1
        url = next(iter(source_urls))
        assert json.loads(source.read_text())['owned_original'] == url
        financial, positions = originals[url]
        return deepcopy(financial), deepcopy(positions), {documents[url]['content_sha256']}
    monkeypatch.setattr(builder, 'corporate_source_review_candidates', reader)
    return a, review, new, corporate, old, path, originals


def test_complete_originals_prove_fixed_cap_implication_after_unchanged_physical_transfer(tmp_path, monkeypatch):
    from copy import deepcopy
    import scripts.build_tw_futures_margin_event_candidates as builder
    from stockagent.data.tw_futures_margin_preparation import position_candidate_intervals
    a, review, new, corporate, old, path, _ = complete_original_generation_fixture(tmp_path, monkeypatch)
    original = deepcopy((old, corporate))
    builder.bind_position_generation_constraints(a, review, new, corporate, old, path)
    assert (old, corporate) == original
    proof = json.loads(new[0]['position_generation_constraint_evidence'])
    assert proof['proof_kind'] == 'verified_originals_unchanged_transfer'
    assert set(proof['complete_original_review_sha256s']) == {'prior', 'current'}
    assert proof['prior_constraint_implied_after_physical_transfer']
    assert not proof['legal_notice_revocation_inferred']
    common = dict(source_content_sha256='b' * 64, source_url='new', issue_date_bound=True,
        known_at='2024-08-28T23:59:59+08:00', published_date='2024-08-28')
    intervals, issues = position_candidate_intervals([*old, *[dict(r, **common) for r in new]])
    assert not issues
    assert {r['product']: r['position_unit'] for r in intervals} == {
        'LVF': 2000., 'LV1': 2000., 'LV2': 2040.}


@pytest.mark.parametrize('problem', [
    'missing_review', 'wrong_review_sha', 'wrong_original_sha', 'quantity_mismatch',
    'missing_components', 'uninspected_cap', 'uninspected_member', 'uninspected_financial_identity',
    'ambiguous_financial_quantity', 'uncovered_month', 'grade_may_change', 'different_phase',
    'different_direction', 'unsupported_proof_kind',
])
def test_complete_original_implication_rejects_unverified_source_or_constraint(tmp_path, monkeypatch, problem):
    import scripts.build_tw_futures_margin_event_candidates as builder
    a, review, new, corporate, old, path, originals = complete_original_generation_fixture(tmp_path, monkeypatch)
    proof = review['prior_generation_constraints'][0]
    if problem == 'missing_review': proof.pop('prior_source_review')
    if problem == 'wrong_review_sha': proof['current_source_review']['sha256'] = 'c' * 64
    if problem == 'wrong_original_sha': originals['old'][0][0]['source_content_sha256'] = 'c' * 64
    if problem == 'quantity_mismatch': originals['new'][0][0]['deliverable_security_quantity'] = 1999.
    if problem == 'missing_components': originals['old'][0][0]['deliverable_components_resolved'] = False
    if problem == 'uninspected_cap': originals['old'][1][0]['natural_person_limit'] = 3999999.
    if problem == 'uninspected_member': originals['new'][1].pop()
    if problem == 'uninspected_financial_identity': originals['new'][0][0]['from_product'] = 'XXF'
    if problem == 'ambiguous_financial_quantity':
        corporate.append(dict(corporate[0], contract_multiplier=1999., deliverable_security_quantity=1999.))
    if problem == 'uncovered_month':
        originals['old'][0][0]['contract_months'] = ['202412']
    if problem == 'grade_may_change':
        for row in new + old: row['limit_follows_applicable_grade'] = True
    if problem == 'different_phase': new[0]['effective_phase'] = 'new_contract_listing'
    if problem == 'different_direction': new[0]['direction'] = 'a different directional aggregation'
    if problem == 'unsupported_proof_kind': proof['proof_kind'] = 'latest_smallest_limit'
    with pytest.raises(ValueError):
        builder.bind_position_generation_constraints(a, review, new, corporate, old, path)
