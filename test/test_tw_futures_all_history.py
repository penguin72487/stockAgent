from datetime import date
import sqlite3
import json
from types import SimpleNamespace

import polars as pl
import pytest
import numpy as np

from scripts.extract_taifex_rule_review_candidates import review_documents
from scripts.build_tw_futures_margin_event_candidates import candidate_notice_clock
from scripts.prepare_tw_futures_physical_history import raw_physical_observations
from stockagent.data.tw_futures_margin_preparation import (
    physical_lifetime_calendar, margin_table_candidates, margin_grid_candidates,
    position_grid_candidates, corporate_grid_candidates, corporate_text_candidates,
    margin_legacy_word_levels, position_legacy_word_candidates,
    corporate_native_table_candidates,
    corporate_deliverable_components,
    corporate_terms_intervals, margin_candidate_intervals, stock_futures_cash_reform_intervals,
    corporate_position_table_candidates,position_prose_candidates,position_candidate_intervals,
)
from stockagent.data.tw_futures_portfolio_daily import (
    _fixed_fee_contract_metadata, _validate_fixed_contract_units,
)


def test_ocr_label_normalization_does_not_repair_numbers_or_contract_codes():
    from stockagent.data.tw_futures_margin_preparation import compact
    assert compact('契约乘数 调整为 2.000 GI1 G1I') == '契約乘數調整為2.000GI1G1I'
    original=[['契約代號','LGF調整為LG1'],['契約乘數','LG1契約乘數調整為2,100'],
              ['約定標的物','調整為2,100股標的證券'],['調整契約月份','104年9月及12月到期契約'],
              ['調整生效日','104年8月28日']]
    simplified=[[s.translate(str.maketrans('約數為標證調','约数为标证调')) for s in row] for row in original]
    assert corporate_grid_candidates(original)==corporate_grid_candidates(simplified)


@pytest.mark.parametrize('row_label',['保證金','比例'])
@pytest.mark.parametrize('product,name',[('JNF','TPK-KY期貨'),('EPF','亞德客-KY期貨'),('FPF','三陽期貨')])
def test_named_margin_blocks_keep_contract_code_separate_from_issuer_english_words(row_label,product,name):
    text=f'''單位：比例（%）
    調整後保證金適用比例 調整前保證金適用比例
    {product} （級距二） （級距一）
    ({name}) 原始保證金 維持保證金 結算保證金 原始保證金 維持保證金 結算保證金
    {row_label} 16.20% 12.42% 12.00% 13.50% 10.35% 10.00%
    '''
    row,=margin_table_candidates(text)
    assert row['product']==product
    assert row['before']==[.135,.1035,.1]
    assert row['after']==[.162,.1242,.12]


def test_cash_reform_transfers_embedded_cash_once_without_code_or_quantity_change():
    row=dict(product='DL2',from_product='DL1',contract='201106',effective_date='2011-01-07',
        valid_until_exclusive=None,deliverable_cash_twd=12120.,contract_multiplier=1600.,
        known_at='2010-12-29T23:59:59+08:00',source_content_sha256s=['a'*64],source_urls=['old'],
        subscription_rights_at_final_settlement=False,equity_cash_credit_twd=None,
        has_equity_credit_fields=False)
    reform=dict(effective_date='2011-05-03',effective_at='2011-05-03T08:30:00+08:00',
        known_at='2011-02-01T23:59:59+08:00',source_content_sha256='b'*64,
        source_content_sha256s=['b'*64,'c'*64],source_urls=['reform','clock'])
    rows,issues=stock_futures_cash_reform_intervals([row],reform,{'a'*64:'cash_capital_return'})
    assert not issues and len(rows)==2
    old,new=rows
    assert old['valid_until_exclusive']=='2011-05-03'
    assert new['from_product']==new['product']=='DL2'
    assert new['deliverable_cash_twd']==0. and new['equity_cash_credit_twd']==12120.
    assert new['contract_multiplier']==old['contract_multiplier']==1600.
    assert new['carry_quantity_numerator']==new['carry_quantity_denominator']==1
    assert new['source_content_sha256s']==['a'*64,'b'*64,'c'*64]
    assert row['valid_until_exclusive'] is None  # Input evidence is immutable.
    # Reapplying the resolver cannot duplicate credits or split its own output.
    assert stock_futures_cash_reform_intervals(rows,reform,{'a'*64:'cash_capital_return'})==(rows,[])
    # Unknown/other cash benefits are a barrier, never a made-up zero cash leg.
    blocked,issues=stock_futures_cash_reform_intervals([row],reform,{})
    assert len(blocked)==1 and blocked[0]['valid_until_exclusive']=='2011-05-03'
    assert issues[0]['reasons']=='cash_reform_origin_requires_source_review'
    expired=dict(row,contract='201103')
    assert stock_futures_cash_reform_intervals([expired],reform,{})==([expired],[])
    superseded=dict(row,valid_until_exclusive='2011-04-25')
    assert stock_futures_cash_reform_intervals([superseded],reform,{})==([superseded],[])
    with pytest.raises(ValueError,match='public'):
        stock_futures_cash_reform_intervals([row],dict(reform,known_at='2011-05-03T23:59:59+08:00'),{})


def test_scanned_corporate_text_keeps_cash_dates_and_deliverables_distinct():
    text = '''調整生效日：107年6月25日。調整契約月份：107年7月、8月、9月及12月到期契約。
    契約代號 不調整(仍為CDF) 約定標的物 不調整(仍為2,000股台積電股票)
    契約乘數 不調整(仍為2,000)
    買方權益數加項 調整生效日加新臺幣16,000元
    賣方權益數減項 調整生效日減新臺幣16,000元
    註1：若標的公司於調整生效日後(含當日)變更股利，則本契約不予調整。
    選擇權調整 契約代號 CDO調整為CD1 契約乘數 調整為9,999
    '''
    fact, = corporate_text_candidates(text)
    assert fact['contract_multiplier'] == 2000.
    assert fact['contract_months'] == ['201807','201808','201809','201812']
    assert fact['effective_date'] == '2018-06-25'
    assert fact['equity_credit_long_per_contract'] == 16000
    assert fact['cash_equity_pair_agrees'] and not fact['includes_deliverable_cash']
    assert fact['later_dividend_revisions_do_not_restate']
    assert not fact['has_conditional_language']
    assert fact['candidate_only'] and fact['requires_phase_and_condition_review']
    broken = corporate_text_candidates(text.replace('107年6月25日', '107年6月35日'))[0]
    assert broken['effective_date'] is None
    assert broken['effective_date_error'] == 'invalid_source_effective_date'
    # OCR guesses are never repairs to an observed cash amount or multiplier.
    assert corporate_text_candidates(text.replace('新臺幣16,000元', '新臺幣I6,000元'))[0]['equity_credit_long_per_contract'] is None
    assert not corporate_text_candidates(text.replace('仍為2,000)', '仍為2,OOO)'))


def test_deliverable_cash_and_subscription_rights_are_separate_components():
    rights=corporate_deliverable_components('調整為2,000股標的證券及其可獲優先參與現金增資之相當價值。',2000.)
    assert rights['deliverable_cash_twd']==0
    assert rights['subscription_rights_at_final_settlement']
    assert rights['deliverable_components_resolved']
    legacy=corporate_deliverable_components('調整為1,600股標的證券及新臺幣12,120元。',1600.)
    assert legacy['deliverable_cash_twd']==12120
    assert not legacy['subscription_rights_at_final_settlement']
    assert legacy['deliverable_components_resolved']
    fractional=corporate_deliverable_components('調整為2,039.46股標的證券及現金新臺幣4,735元',2039.46)
    assert fractional['deliverable_components_resolved']
    assert fractional['deliverable_security_quantity']==2039.46
    bad=corporate_deliverable_components('2.16股標的證券及其可獲優先參與現金增資之相當價值',2016.)
    assert not bad['deliverable_components_resolved']
    assert not corporate_deliverable_components('調整為2,000股標的證券及現金新臺幣I,OOO元',2000.)['deliverable_components_resolved']
    assert corporate_deliverable_components('仍為2,000股台積電股票',2000.)['deliverable_components_resolved']
    assert not corporate_deliverable_components('2,000股甲公司普通股及2,000股乙公司普通股',2000.)['deliverable_components_resolved']
    prior=corporate_deliverable_components('調整為2,135.5023股標的證券及原2,000股標的證券可獲優先參與現金增資之相當價值',2135.5023)
    assert prior['deliverable_components_resolved'] and prior['subscription_rights_at_final_settlement']
    repeated=corporate_deliverable_components('2,120股標的證券及2,120股標的證券可獲優先參與現金增資之相當價值',2120.)
    assert repeated['deliverable_components_resolved']
    assert repeated['deliverable_security_quantity']==2120.


def test_legacy_adjusted_header_labels_preserve_actual_numeric_cells():
    cells=[['調整契約代號','ABF調整為AB1'],['調整約定標的物','2,040股標的證券'],
           ['契約乘數','AB1契約乘數調整為2,040']]
    fact,=corporate_grid_candidates(cells)
    assert fact['product']=='AB1' and fact['contract_multiplier']==2040.
    assert fact['deliverable_components_resolved']


def test_partial_corporate_review_preserves_sibling_products_and_other_sources():
    from scripts.build_tw_futures_margin_event_candidates import replace_reviewed_product_facts
    existing=[dict(product='AB1',source_content_sha256='a',value=1),
              dict(product='AB2',source_content_sha256='a',value=2),
              dict(product='AB1',source_content_sha256='b',value=3)]
    reviewed=[dict(product='AB1',source_content_sha256='a',value=4)]
    result=replace_reviewed_product_facts(existing,reviewed)
    assert sorted(r['value'] for r in result)==[2,3,4]
    assert replace_reviewed_product_facts(existing,[])==existing


def test_fixed_rights_use_decimal_source_operands_and_reject_future_price():
    from scripts.build_tw_futures_margin_event_candidates import reviewed_fixed_subscription_rights
    fact=dict(product='JA2',deliverable_cash_twd=0.,subscription_rights_at_final_settlement=True)
    component=dict(product='JA2',page=2,rounding='floor_twd',observation_date='2014-02-21',
        closing_price='16',subscription_price='14.5',subscription_quantity='218.0736',
        transcribed_formula='收盤價(16元)與認購價(14.5元)之差額，乘以218.0736')
    review=dict(visually_reviewed_pages=[2],published_date='2014-08-20',fixed_subscription_rights=[component])
    result=reviewed_fixed_subscription_rights([dict(fact)],review)
    assert result[0]['deliverable_cash_twd']==0.  # Rights do not enter daily marks.
    assert result[0]['fixed_subscription_rights_twd']==327.
    assert result[0]['subscription_rights_at_final_settlement']
    component['observation_date']='2014-09-17'
    with pytest.raises(ValueError,match='prior-observable'):
        reviewed_fixed_subscription_rights([dict(fact)],review)


def test_corporate_fields_can_bind_complete_sentences_when_scanned_labels_are_lost():
    cells=[['契約代號','HN1調整為HN2'],['','HN2契約乘數不調整(仍為2,120.1821)'],
        ['約定標的物','調整為2,120.1821股標的證券'],['調整契約月份','101年3月到期契約'],
        ['調整生效日','101年3月5日'],
        ['','每口買方未沖銷部位調整買方權益數加項新臺幣400元'],
        ['','每口賣方未沖銷部位調整賣方權益數減項新臺幣400元']]
    fact,=corporate_grid_candidates(cells)
    assert fact['contract_multiplier']==2120.1821
    assert fact['cash_equity_pair_agrees'] and fact['equity_credit_long_per_contract']==400
    assert fact['contract_months']==['201203']
    wrong_code=[row if i!=1 else ['',row[1].replace('HN2','HN1')] for i,row in enumerate(cells)]
    assert not corporate_grid_candidates(wrong_code)
    assert not corporate_grid_candidates(cells+[['契約乘數','調整為2,000']])
    assert not corporate_grid_candidates(cells+[['契約代號','HNF調整為HN1']])


def test_flattened_corporate_month_scope_stops_before_bound_destination_prefix():
    text='''調整生效日：101年3月5日。契約代號HN1調整為HN2
    調整契約月份101年3月到期契約HN2約定標的物調整為2,120.1821股標的證券
    契約乘數HN2契約乘數不調整(仍為2,120.1821)'''
    fact,=corporate_text_candidates(text)
    assert fact['contract_months']==['201203']
    assert fact['contract_months_error'] is None
    # A different code or stray number is not discarded to make the date fit.
    for ending in ('HN1','2'):
        broken=corporate_text_candidates(text.replace('到期契約HN2','到期契約'+ending))[0]
        assert not broken['contract_months']


def test_corporate_month_scope_enrichment_requires_identical_source_event():
    common=dict(product='JM2',from_product='JM1',effective_date='2011-10-19',contract_months=[],
        issue_date_bound=True,known_at='2011-10-01T23:59:59+08:00',contract_multiplier=2199.9843,
        source_content_sha256='a'*64,source_url='official-a',has_equity_credit_fields=False,
        **corporate_deliverable_components('2,199.9843股標的證券',2199.9843))
    scope=dict(common,contract_months=['201203'],deliverable_components_resolved=False,
        extraction_method='source_text',page=2)
    rows,issues=corporate_terms_intervals([common,scope])
    assert len(rows)==1 and rows[0]['contract']=='201203'
    assert rows[0]['month_scope_provenance']
    assert common['contract_months']==[]  # The original evidence is immutable.
    for change in ({'source_content_sha256':'b'*64},{'from_product':'JMF'},
                   {'product':'JM1'},{'effective_date':'2011-10-18'}):
        rows,_=corporate_terms_intervals([common,dict(scope,**change)])
        assert not rows
    rows,_=corporate_terms_intervals([common,scope,dict(scope,contract_months=['201112'])])
    assert not rows  # Conflicting source views cannot provide a scope.


def test_corporate_term_conflict_is_a_barrier_not_last_row_wins():
    original=dict(product='DL1',from_product='DLF',effective_date='2011-01-25',contract_months=['201103'],
        issue_date_bound=True,known_at='2011-01-01T23:59:59+08:00',contract_multiplier=1600.,
        source_content_sha256='a'*64,source_url='official-a',has_equity_credit_fields=False,
        **corporate_deliverable_components('1,600股標的證券及新臺幣4,000元。',1600.))
    # Re-extraction of one source agrees and contributes no duplicate interval.
    rows,issues=corporate_terms_intervals([original,dict(original)])
    assert len(rows)==1 and not issues
    later=dict(original,effective_date='2011-02-15',known_at='2011-02-01T23:59:59+08:00',
               source_content_sha256='b'*64,source_url='official-b')
    contradictory=dict(later,deliverable_cash_twd=5000.,source_content_sha256='c'*64,source_url='official-c')
    rows,issues=corporate_terms_intervals([contradictory,original,later])
    assert len(rows)==1 and rows[0]['valid_until_exclusive']=='2011-02-15'
    assert any(i['reasons']=='conflicting_or_unresolved_effective_terms' for i in issues)
    future=dict(original,known_at='2011-01-25T23:59:59+08:00')
    rows,issues=corporate_terms_intervals([future])
    assert not rows and any('not_known_before' in i['reasons'] for i in issues)
    rows,issues=corporate_terms_intervals([dict(original,known_at=None)])
    assert not rows and any('publication_timestamp' in i['reasons'] for i in issues)


def test_explicit_notice_revision_replaces_terms_only_if_known_before_effective_day():
    old=dict(product='JL1',from_product='JLF',effective_date='2011-07-25',contract_months=['201109'],
        issue_date_bound=True,known_at='2011-07-15T23:59:59+08:00',contract_multiplier=2100.,
        source_content_sha256='a'*64,source_url='original',has_equity_credit_fields=False,
        **corporate_deliverable_components('2,100股標的證券',2100.))
    new=dict(old,known_at='2011-07-21T23:59:59+08:00',contract_multiplier=2099.7509,
        source_content_sha256='b'*64,source_url='amendment',
        supersedes_source_sha256s=['a'*64],supersession_review_sha256='c'*64,
        **corporate_deliverable_components('2,099.7509股標的證券',2099.7509))
    for facts in ([old,new],[new,old]):
        rows,issues=corporate_terms_intervals(facts)
        assert len(rows)==1 and not issues
        assert rows[0]['contract_multiplier']==2099.7509
        assert rows[0]['superseded_source_sha256s']==['a'*64]
    for change in ({'supersedes_source_sha256s':[]}, {'known_at':'2011-07-25T23:59:59+08:00'}):
        rows,issues=corporate_terms_intervals([old,dict(new,**change)])
        assert not rows and issues
    with pytest.raises(ValueError,match='reviewed source evidence'):
        corporate_terms_intervals([old,dict(new,supersession_review_sha256=None)])
    # The same source revision also changes its dated securities position cap.
    position=dict(old,event_type='corporate_securities_unit_limit',unit='shares',
        position_unit=2100.,natural_person_limit=735000,combined_products=['JLF','JL1'],
        effective_phase='product_regular_open')
    amended=dict(position,known_at=new['known_at'],source_content_sha256='b'*64,
        position_unit=2099.7509,natural_person_limit=734913,
        supersedes_source_sha256s=['a'*64],supersession_review_sha256='c'*64)
    rows,issues=position_candidate_intervals([position,amended])
    assert len(rows)==1 and not issues
    assert rows[0]['position_limit']==734913 and rows[0]['position_unit']==2099.7509


def test_outgoing_corporate_transfer_ends_old_code_even_with_unresolved_destination():
    old=dict(product='CN1',from_product='CNF',effective_date='2012-09-18',contract_months=['201303'],
        issue_date_bound=True,known_at='2012-09-04T23:59:59+08:00',contract_multiplier=2176.,
        source_content_sha256='a'*64,source_url='official-a',has_equity_credit_fields=False,
        **corporate_deliverable_components('2,176股標的證券',2176.))
    outgoing=dict(old,product='CN2',from_product='CN1',effective_date='2013-02-19',
        source_content_sha256='b'*64,source_url='official-b',deliverable_components_resolved=False)
    rows,issues=corporate_terms_intervals([old,outgoing])
    assert len(rows)==1 and rows[0]['valid_until_exclusive']=='2013-02-19'
    assert rows[0]['end_includes_outgoing_transfer']
    assert any('deliverable_components_unresolved' in r['reasons'] for r in issues)
    # Reusing CN1 on that same day is a new incoming state, not a reason to
    # continue its old quantity. The uncertain CN2 remains unadmitted.
    fresh=dict(old,effective_date='2013-02-19',source_content_sha256='c'*64,
        contract_multiplier=2000.,**corporate_deliverable_components('2,000股標的證券',2000.))
    rows,issues=corporate_terms_intervals([fresh,outgoing,old])
    cn=[r for r in rows if r['product']=='CN1']
    assert len(cn)==2 and cn[0]['valid_until_exclusive']=='2013-02-19'
    assert cn[1]['contract_multiplier']==2000


def test_corporate_source_views_enrich_absent_credit_without_zero_or_order_priority():
    common=dict(product='DL1',from_product='DLF',effective_date='2012-07-25',contract_months=['201209'],
        issue_date_bound=True,known_at='2012-07-01T23:59:59+08:00',contract_multiplier=2000.,
        source_content_sha256='a'*64,source_url='official-a',has_equity_credit_fields=False,
        **corporate_deliverable_components('2,000股標的證券',2000.))
    complete=dict(common,has_equity_credit_fields=True,cash_equity_pair_agrees=True,
                  equity_credit_long_per_contract=2000.,equity_debit_short_per_contract=2000.)
    for rows in ([common,complete],[complete,common]):
        intervals,issues=corporate_terms_intervals(rows)
        assert len(intervals)==1 and not issues
        assert intervals[0]['equity_cash_credit_twd']==2000.
    conflicting=dict(complete,equity_credit_long_per_contract=3000.,equity_debit_short_per_contract=3000.)
    intervals,issues=corporate_terms_intervals([common,complete,conflicting])
    assert not intervals and any('conflicting' in r['reasons'] for r in issues)
    # No enrichment across independent documents; absence is not a waiver.
    intervals,issues=corporate_terms_intervals([common,dict(complete,source_content_sha256='b'*64)])
    assert not intervals and any('conflicting' in r['reasons'] for r in issues)


def test_margin_intervals_stop_at_unproved_restoration_and_reanchor_at_explicit_level():
    common=dict(product='CZF',margin_kind='notional_rate',after=[.162,.1242,.12],before=None,
        effective_date='2024-04-01',effective_phase='after_product_regular_close',
        published_date='2024-03-28',known_at='2024-03-28T23:59:59+08:00',
        issue_date_bound=True,source_content_sha256='a'*64,source_url='official-a')
    temporary=dict(common,requires_reversion_review=True,temporary_end_evidence=json.dumps([
        dict(date_iso='2024-04-08',boundary='after_regular_session'),
        dict(date_iso='2024-04-02',boundary='after_regular_session')]))
    later=dict(common,source_content_sha256='b'*64,source_url='official-b',
        effective_date='2024-04-10',known_at='2024-04-09T23:59:59+08:00',
        before=[.135,.1035,.1],after=[.2025,.1553,.15])
    intervals,issues=margin_candidate_intervals([later,temporary])
    assert not issues and len(intervals)==2
    assert intervals[0]['valid_until_date_exclusive']=='2024-04-02'
    assert intervals[0]['valid_until_phase_exclusive']==1
    assert all(not row['restoration_applied'] and not row['point_in_time_verified'] for row in intervals)
    assert intervals[1]['effective_date']=='2024-04-10'
    intervals,issues=margin_candidate_intervals([dict(temporary,temporary_end_evidence='[]')])
    assert not intervals and any('restoration_rule_unresolved' in row['reasons'] for row in issues)


def test_adjusted_position_caps_use_explicit_shares_and_preserve_delisting_clock():
    pages=[dict(page=1,native_text='DA1與DAF部位合併計算',tables=[
        dict(cells=[['契約代號','DAF調整為DA1'],['約定標的物','調整為2,200股標的證券'],
                    ['契約乘數','DA1契約乘數調整為2,200']],caption=''),
        dict(cells=[['持有部位','DAO','DAA','DAF','DA1'],['每口折算股數','2,000','2,200','2,000','2,200']],caption='')]),
        dict(page=2,native_text='',tables=[dict(cells=[
            ['適用期間','自100.09.22起至100.11.16(100年11月契約到期日)止','自100.11.17起至DAA、DA1契約終止掛牌前一營業日止'],
            ['自然人','5,500,000股','5,000,000股'],['法人機構','16,500,000股','15,000,000股']],caption='')])]
    rows=corporate_position_table_candidates(pages)
    assert len(rows)==4 and {r['product'] for r in rows}=={'DAF','DA1'}
    first=[r for r in rows if r['effective_date']=='2011-09-22']
    assert all(r['natural_person_limit']==5500000 and r['valid_until_date_inclusive']=='2011-11-16' for r in first)
    assert {r['product']:r['position_unit'] for r in first}=={'DAF':2000.,'DA1':2200.}
    assert all(r['requires_delisting_clock'] and r['valid_until_date_inclusive'] is None
               for r in rows if r['effective_date']=='2011-11-17')
    pages[0]['native_text']='DAA與DAO同方向選擇權部位合併計算'
    assert not corporate_position_table_candidates(pages)


def test_margin_chain_disagreement_invalidates_prior_interval_without_backdating_new_level():
    first=dict(product='TX',margin_kind='fixed_twd',after=[100000,77000,74000],before=None,
        effective_date='2024-04-01',effective_phase='after_product_regular_close',
        published_date='2024-03-28',known_at='2024-03-28T23:59:59+08:00',
        issue_date_bound=True,source_content_sha256='a'*64,source_url='official-a')
    later=dict(first,effective_date='2024-05-01',known_at='2024-04-30T23:59:59+08:00',
        before=[110000,85000,81000],after=[120000,93000,89000],
        source_content_sha256='b'*64,source_url='official-b')
    rows,issues=margin_candidate_intervals([first,later])
    assert len(rows)==1 and rows[0]['effective_date']=='2024-05-01'
    assert any(row['reasons']=='prior_interval_before_after_chain_disagrees' for row in issues)
    # A second source that contradicts the new level creates a barrier.
    rows,issues=margin_candidate_intervals([first,dict(later,before=first['after']),
        dict(later,source_content_sha256='c'*64,after=[140000,110000,100000])])
    assert len(rows)==1 and rows[0]['valid_until_date_exclusive']=='2024-05-01'
    assert any(row['reasons']=='conflicting_or_unresolved_margin_boundary' for row in issues)
    # A malformed, same-source view does not override an independently intact view.
    rows,issues=margin_candidate_intervals([first,dict(first,after=[])])
    assert len(rows)==1
    rows,issues=margin_candidate_intervals([dict(first,effective_date='bad',published_date='bad')])
    assert not rows and issues[0]['reasons']=='unlocatable_source_boundary'
    rows,issues=margin_candidate_intervals([dict(first,known_at='2024-04-01T23:59:59+08:00')])
    assert len(rows)==1 and not issues and rows[0]['requires_delayed_admission']
    assert rows[0]['known_at']=='2024-04-01T23:59:59+08:00'
    assert rows[0]['effective_date']=='2024-04-01'
    rows,issues=margin_candidate_intervals([dict(first,known_at='2024-05-02T23:59:59+08:00'),
        dict(later,before=first['after'])])
    assert len(rows)==1 and rows[0]['effective_date']=='2024-05-01'
    assert any(r['reasons']=='rule_became_known_after_interval_ended' for r in issues)


def test_legacy_listing_positions_keep_futures_date_despite_option_immediate_amendment():
    text=('主旨：99年1月25日新上市35檔「股票期貨契約」部位限制契約數。'
          '公告事項：股票選擇權部位自即日起實施。自然人。\n'
          '附件 1\n99年1月25日新上市35檔「股票期貨契約」\n'
          '|CDF|台積電期貨|1|5,000|15,000|\n|CDO|台積電選擇權|1|9,999|15,000|')
    row,=position_legacy_word_candidates(text)
    assert (row['product'],row['effective_date'],row['effective_phase'])==('CDF','2010-01-25','new_contract_listing')
    assert row['natural_person_limit']==5000


def test_position_prose_uses_clause_dates_and_never_options_or_mini_aggregation():
    text=('發文日期：中華民國98年11月3日。部位限制公告事項：'
          '1.自即日起，「臺股期貨」調整為自然人5,000個契約，法人10,000個契約。'
          '2.自98年12月17日起，「電子期貨」、「金融期貨」調整為自然人300個契約；'
          '「臺幣黃金期貨」調整為自然人500個契約；「臺指選擇權」自然人25,000個契約。正本：')
    rows={r['product']:r for r in position_prose_candidates(text)}
    assert set(rows)=={'TX','TE','TF','TGF'}
    assert rows['TX']['effective_date']=='2009-11-03'
    assert all(rows[p]['effective_date']=='2009-12-17' for p in ('TE','TF','TGF'))
    assert rows['TE']['natural_person_limit']==300 and rows['TGF']['natural_person_limit']==500
    assert all(r['effective_phase']=='date_only_requires_phase_review' for r in rows.values())
    assert not position_prose_candidates('部位限制公告事項：「臺股期貨」加計依合約規模調整後之'
        '「小型臺指期貨」：自然人1,800個契約。')


def test_position_intervals_preserve_dual_axes_formula_and_knowledge_boundary():
    base=dict(product='CPF',natural_person_limit=2000,natural_person_monthly_limit=500,
        unit='contracts',event_type='absolute_level',effective_date='2007-01-15',
        effective_phase='product_regular_open',issue_date_bound=True,published_date='2007-01-10',
        known_at='2007-01-10T23:59:59+08:00',source_content_sha256='a'*64,source_url='official-a')
    formula=dict(base,product='MTX',natural_person_limit=None,natural_person_monthly_limit=None,
        event_type='combined_position_formula',combined_position_base_product='TX',combined_position_ratio='1/4')
    rows,issues=position_candidate_intervals([base,formula])
    assert not issues and len(rows)==2
    cpf,mini=rows
    assert cpf['position_limit']==2000 and cpf['monthly_position_limit']==500
    assert mini['position_limit'] is None and mini['requires_base_limit_join']
    assert (mini['conversion_numerator'],mini['conversion_denominator'])==(1,4)
    delayed=dict(base,effective_phase='date_only_requires_phase_review',known_at='2007-01-15T23:59:59+08:00')
    rows,issues=position_candidate_intervals([delayed])
    assert not issues and rows[0]['admission_not_before']=='2007-01-15T23:59:59+08:00'
    assert rows[0]['effective_date']=='2007-01-15' and rows[0]['legal_date_only']
    rows,issues=position_candidate_intervals([dict(formula,combined_position_base_product=None)])
    assert not rows and any('dated_base_product_identity_unresolved' in r['reasons'] for r in issues)


def test_position_share_caps_have_bounded_periods_and_conflicts_stop_prior_state():
    first=dict(product='DA1',natural_person_limit=5500000,unit='shares',position_unit=2200.,
        combined_products=['DAF','DA1'],event_type='corporate_securities_unit_limit',
        effective_date='2011-09-22',effective_phase='product_regular_open',issue_date_bound=True,
        published_date='2011-09-01',known_at='2011-09-01T23:59:59+08:00',
        source_content_sha256='a'*64,source_url='official-a',valid_until_date_inclusive='2011-11-16')
    later=dict(first,effective_date='2011-11-17',natural_person_limit=5000000,
        valid_until_date_inclusive=None,requires_delisting_clock=True)
    rows,issues=position_candidate_intervals([first,later])
    assert not issues and len(rows)==2 and rows[0]['valid_until_date_exclusive']=='2011-11-17'
    assert rows[1]['requires_delisting_clock'] and rows[1]['valid_until_date_exclusive'] is None
    assert all(r['position_unit']==2200 and r['requires_dated_group_and_lifecycle_admission'] for r in rows)
    rows,issues=position_candidate_intervals([first,later,dict(later,natural_person_limit=6000000,
        source_content_sha256='b'*64)])
    assert len(rows)==1 and rows[0]['valid_until_date_exclusive']=='2011-11-17'
    assert any(r['reasons']=='conflicting_or_unresolved_position_boundary' for r in issues)


def test_native_refresh_preserves_independent_text_candidates():
    from scripts.build_tw_futures_margin_event_candidates import refresh_corporate_candidates
    archive = SimpleNamespace(sources={})
    row = dict(extraction_method='explicit_text_fields_requires_source_review',
               source_content_sha256='synthetic-no-cell-grid')
    assert refresh_corporate_candidates(archive, [row]) == 0
    assert row['source_content_sha256'] == 'synthetic-no-cell-grid'


def test_all_retained_ocr_views_are_receipt_bound_and_order_independent(tmp_path):
    from scripts.build_tw_futures_margin_event_candidates import retained_document_text_views
    from downloader.artifact_io import sha256_file
    sources={}
    for i in range(2):
        text=tmp_path/f'text{i}.txt';text.write_text(f'independent OCR view {i}')
        receipt=tmp_path/f'receipt{i}.json'
        receipt.write_text(json.dumps(dict(content_sha256='a'*64,status='complete',
            files=[dict(path='candidate.txt',sha256=sha256_file(text))])))
        for path,kind in [(text,'review_pages_including_ocr'),(receipt,'page_extraction_receipt')]:
            sources[path.name]=dict(path=path.name,sha256=sha256_file(path),url='official',kind=kind)
    document=dict(content_sha256='a'*64,text='native text')
    archive=SimpleNamespace(bundle=tmp_path,sources=sources)
    first=retained_document_text_views(archive,'official',document)
    archive.sources=dict(reversed(list(sources.items())))
    assert retained_document_text_views(archive,'official',document)==first
    assert {text for text,kind in first}=={'native text','independent OCR view 0','independent OCR view 1'}
    (tmp_path/'text0.txt').write_text('changed after receipt')
    with pytest.raises(ValueError,match='SHA mismatch'):
        retained_document_text_views(archive,'official',document)


@pytest.mark.parametrize('scan_gaps',[False,True])
def test_ocr_geometry_preserves_cell_assignment_and_ocr_provenance(scan_gaps):
    cv2=pytest.importorskip('cv2')
    from scripts.extract_taifex_rule_review_candidates import ruled_ocr_tables
    page=np.full((1000,1000),255,dtype=np.uint8)
    for y in (300,420,540):cv2.line(page,(100,y),(900,y),0,2)
    for x in (100,400,900):cv2.line(page,(x,300),(x,540),0,2)
    if scan_gaps:
        for y in (300,420,540):
            for x in (150,250,350,550,750):page[y-2:y+3,x:x+2]=255
        for x in (100,400,900):
            for y in (330,390,470,510):page[y:y+2,x-2:x+3]=255
    def token(text,x,y,w=80):
        return dict(txt=text,box=[[x,y],[x+w,y],[x+w,y+20],[x,y+20]],score=.99)
    tables=ruled_ocr_tables(page,[token('left1',120,340),token('right1',450,340),
        token('left2',120,460),token('right2',450,460),token('crossing',370,390),
        token('caption',100,250)])
    assert len(tables)==1
    assert tables[0]['cells']==[['left1','right1'],['left2','right2']]
    assert tables[0]['caption']=='caption'
    assert tables[0]['extraction_method']=='source_ruled_ocr_cell_grid'
    assert tables[0]['excluded_boundary_tokens']==[4]


def test_corporate_repeated_label_in_explicit_equity_formula():
    text='''調整生效日：102年8月8日 調整契約月份：102年8月、9月到期契約
    契約代號 KWF調整為KW1 約定標的物 調整為2,020股標的證券
    契約乘數 KW1契約乘數調整為2,020
    買方權益數加項 | 每口買方未沖銷部位調整買方權益數加項新臺幣600元
    賣方權益數減項 | 每口賣方未沖銷部位調整賣方權益數減項新臺幣600元
    二、加掛標準契約：契約代號KWF 契約乘數2,000'''
    fact,=corporate_text_candidates(text)
    assert fact['contract_multiplier']==2020
    assert fact['equity_credit_long_per_contract']==600
    assert fact['equity_debit_short_per_contract']==600
    # Separate repeated amount rows are still ambiguous, not last-value wins.
    bad=text.replace('二、加掛','買方權益數加項 新臺幣900元 二、加掛')
    assert corporate_text_candidates(bad)[0]['equity_credit_long_per_contract'] is None


def test_margin_footer_and_legacy_currency_blocks():
    text='''單位：新臺幣元
    CPF 調整後保證金金額 調整前保證金金額
    原始保證金 維持保證金 結算保證金 原始保證金 維持保證金 結算保證金
    保證金 15,000 12,000 11,000 13,000 10,000 9,000
    第 1 頁，共 1 頁'''
    assert margin_table_candidates(text)[0]['after']==[15000,12000,11000]
    legacy='''|單位：新臺幣元|
|CPF|調整後保證金金額|調整前保證金金額|
||原始|維持|結算|原始|維持|結算|
|保證金|一萬五千元|一萬二千元|一萬一千元|一萬一千元|九千元|八千元|
|單位：美元|
|GDF|調整後保證金金額|調整前保證金金額|
||原始|維持|結算|原始|維持|結算|
|保證金|1000|800|700|900|700|600|
'''
    rows=margin_legacy_word_levels(legacy)
    assert [(r['product'],r['margin_kind']) for r in rows]==[('CPF','fixed_twd'),('GDF','fixed_usd')]
    assert rows[0]['after']==[15000,12000,11000]
    spec='''|英文代碼|CPF|
|保證金|原始保證金每單位新臺幣一萬七千元、維持保證金每單位新臺幣一萬三千元、結算保證金每單位新臺幣一萬一千元|
'''
    assert margin_legacy_word_levels(spec)[0]['after']==[17000,13000,11000]


def test_listing_date_and_native_market_position_scope():
    clock=candidate_notice_clock('發文日期：中華民國99年1月14日。公告本公司99年1月25日新上市35檔「股票期貨契約」之交易人部位限制數。',
                                 '2010-01-15')
    assert clock['effective_date']=='2010-01-25' and clock['effective_phase']=='new_contract_listing'
    rows=position_grid_candidates([
        ['商品別','自然人','法人'],['商業本票期貨(CPF)','單一月份500，各月份合計2,000','10000'],
        ['臺指選擇權(TXO)','10000','10000']],
        '發文日期：中華民國96年1月10日。單位：契約數，自96年1月15日起生效。')
    assert len(rows)==1 and rows[0]['product']=='CPF'
    assert rows[0]['natural_person_monthly_limit']==500 and rows[0]['natural_person_limit']==2000
    assert rows[0]['effective_date']=='2007-01-15'


def test_mixed_final_settlement_keeps_physical_delivery_out_of_legacy_cash_reader(tmp_path):
    from downloader.artifact_io import sha256_file
    from stockagent.data.tw_futures_margin_preparation import load_preparation_final_settlements
    from stockagent.data.tw_stock_futures_carry import load_final_settlements
    raw=tmp_path/'source.html';raw.write_text('official interest-rate source fixture')
    path=tmp_path/'final.parquet'
    pl.DataFrame(dict(product=['GBF','CPF'],contract=['201006','201006'],
        settlement_date=[date(2010,6,9),date(2010,6,16)],
        final_settlement_price=[100.,99.],final_settlement_value=[None,None],
        settlement_method=['physical_delivery','cash_settlement'],
        reported_date_role=['last_trading_day','last_trading_day'],source_sha256=[sha256_file(raw)]*2)).write_parquet(path)
    manifest=dict(schema_version=2,status='complete',requires_product_specific_settlement_clock=True,
        receipts=[dict(path=raw.name,sha256=sha256_file(raw))],
        outputs={'futures_final_settlement_history':dict(sha256=sha256_file(path))})
    path.with_name('manifest.json').write_text(json.dumps(manifest))
    frame=load_preparation_final_settlements(path)
    assert frame['settlement_method'].to_list()==['physical_delivery','cash_settlement']
    obs=pl.DataFrame(dict(product=['GBF'],contract=['201006'],date=[date(2010,6,8)]))
    calendar,lives=physical_lifetime_calendar(obs,frame,pl.DataFrame({'date':[date(2010,6,8),date(2010,6,9)]}))
    assert lives['settlement_method'].to_list()==['physical_delivery']
    assert set(calendar['reported_date_role'])=={'last_trading_day'}
    with pytest.raises(ValueError,match='receipt'):
        load_final_settlements(path)
    raw.write_text('tampered')
    with pytest.raises(ValueError,match='raw source SHA'):
        load_preparation_final_settlements(path)


def test_second_adjusted_contract_cannot_inherit_standard_share_count():
    for code,kind in [('DL1','stock_future'),('DL2','stock_future'),('NY2','etf_future')]:
        multiplier,fee,reason=_fixed_fee_contract_metadata(code,'中華電期貨',kind)
        assert multiplier is None and fee is None and 'historical_notice' in reason
        with pytest.raises(ValueError,match='dated adjusted deliverables'):
            _validate_fixed_contract_units(pl.DataFrame({'product':[code],'contract_multiplier':[2000.]}))
    assert _fixed_fee_contract_metadata('DLF','中華電期貨','stock_future')[0]==2000.
    assert _fixed_fee_contract_metadata('QFF','小型台積電期貨','stock_future')[0]==100.
    _validate_fixed_contract_units(pl.DataFrame({'product':['TX','MTX','T5F','QFF']}))


def test_margin_tables_preserve_product_units_and_column_order():
    template='''單位：{unit}
    {code} 調整後保證金適用比例 調整前保證金適用比例
    原始保證金 維持保證金 結算保證金 原始保證金 維持保證金 結算保證金
    保證金 {amounts}
    '''
    ratio=template.format(unit='比例(%)',code='DJF',amounts='16.20% 12.42% 12.00% 13.50% 10.35% 10.00%')
    fixed=template.format(unit='新臺幣元',code='TE',amounts='110,000 84,000 81,000 100,000 77,000 74,000')
    facts=margin_table_candidates(ratio+fixed)
    assert [f['margin_kind'] for f in facts] == ['notional_rate','fixed_twd']
    assert facts[0]['after'] == [.162,.1242,.12]
    assert facts[1]['after'] == [110000,84000,81000]
    assert all(f['candidate_only'] for f in facts)
    assert not margin_table_candidates(ratio.replace('比例(%)','未知'))
    assert not margin_table_candidates(ratio.replace('調整前','因應春節'))
    assert not margin_table_candidates(ratio.replace('DJF','DJO'))
    assert not margin_table_candidates(ratio.replace('DJF','DJF QRF'))
    assert not margin_table_candidates(ratio.replace('16.20%','16.20'))


def test_native_multi_product_margin_grid_preserves_units_and_roles():
    grid = [
        ['契約名稱','調整後保證金適用比例',None,None,None,'調整前保證金適用比例',None,None,None],
        [None,'級距','原始保證金比例','維持保證金比例','結算保證金比例',
         '級距','原始保證金比例','維持保證金比例','結算保證金比例'],
        ['亞德客-KY期貨(EPF)','2','16.20%','12.42%','12.00%','3','20.25%','15.53%','15.00%'],
        ['長榮期貨(CZF)','-','22.95%','17.60%','17.00%','-','21.60%','16.56%','16.00%'],
        ['長榮選擇權(CZO)','-','22.95%','17.60%','17.00%','-','21.60%','16.56%','16.00%'],
        ['新商品(XYF)','1','20.25%','15.53%','15.00%','1',None,'12.42%','12.00%'],
    ]
    facts = margin_grid_candidates(grid)
    assert [f['product'] for f in facts] == ['EPF','CZF']
    assert facts[0]['after'] == [.162,.1242,.12]
    assert facts[0]['before'] == [.2025,.1553,.15]
    assert all(f['candidate_only'] and f['margin_kind']=='notional_rate' for f in facts)
    fixed = [row[:] for row in grid[:2]] + [
        ['電子期貨(TE)','-','100000','77000','74000','-','110000','84000','81000']]
    assert not margin_grid_candidates(fixed)
    assert margin_grid_candidates(fixed,'單位：美元')[0]['margin_kind']=='fixed_usd'
    assert margin_grid_candidates(fixed,'單位：新臺幣元')[0]['after'] == [100000,77000,74000]
    reversed_header = [row[:] for row in grid]
    reversed_header[0][1],reversed_header[0][5] = reversed_header[0][5],reversed_header[0][1]
    assert margin_grid_candidates(reversed_header)[0]['after'] == facts[0]['before']
    initial = [['期貨英文代碼','結算保證金比例','維持保證金比例','原始保證金比例'],
               ['CAF','10.00%','10.35%','13.50%']]
    level = margin_grid_candidates(initial)[0]
    assert level['event_type']=='absolute_level' and level['before'] is None
    assert level['after']==[.135,.1035,.1]
    initial[0][0]='調整後保證金'
    assert not margin_grid_candidates(initial)
    combined=[row[:] for row in grid]
    combined[2][0]='台積電期貨及選擇權(CDF)(CDO)'
    assert margin_grid_candidates(combined)[0]['product']=='CDF'
    family=[['契約名稱','調整後保證金比例',None,None,'調整前保證金比例',None,None],
            [None,'原始','維持','結算','原始','維持','結算'],
            ['微星期貨及選擇權(GI)','16.2%','12.42%','12%','13.5%','10.35%','10%']]
    assert margin_grid_candidates(family,'股票期貨暨選擇權')[0]['product']=='GIF'
    multi_currency=[['契約名稱','幣別','調整後保證金金額',None,None,'調整前保證金金額',None,None],
                    [None,None,'原始','維持','結算','原始','維持','結算'],
                    ['電子期貨(TE)','新臺幣元','100000','77000','74000','110000','84000','81000'],
                    ['MSCI臺指期貨(MSF)','美元','1350','1040','1000','1485','1144','1100']]
    assert [(f['product'],f['margin_kind']) for f in margin_grid_candidates(multi_currency)]==[
        ('TE','fixed_twd'),('MSF','fixed_usd')]


def test_legacy_market_position_table_preserves_monthly_and_aggregate_caps():
    text='''自96年1月15日起，交易人部位限制數如附件。單位：契約數
|商品別|自然人|法人|期貨自營商|
|台股期貨(TX)|2,000|4,500|13,500|
|小型臺指期貨(MTX)|與台股期貨合併計算﹙依4口小型臺指期貨契約等於1口台股期貨契約合併計算﹚|
|利率期貨|
|公債期貨(GBF)|單一月份1,000，各月份合計2,000|單一月份3,000，各月份合計6,000|
|三十天期利率期貨(CPF)|單一月份500，各月份合計2,000|單一月份1,500，各月份合計6,000|
|臺指選擇權(TXO)|30,000|65,000|195,000|
'''
    rows={r['product']:r for r in position_legacy_word_candidates(text)}
    assert set(rows)=={'TX','MTX','GBF','CPF'}
    assert rows['GBF']['natural_person_monthly_limit']==1000
    assert rows['CPF']['natural_person_monthly_limit']==500
    assert rows['CPF']['natural_person_limit']==2000
    assert rows['MTX']['combined_position_base_product']=='TX'
    assert rows['MTX']['combined_position_ratio']=='1/4'
    assert all(r['effective_date']=='2007-01-15' for r in rows.values())


def test_candidate_clock_does_not_assign_all_products_tx_close_or_hide_reversion():
    text='''臺灣期貨交易所 新聞稿 中華民國111年1月18日
    自111年1月19日一般交易時段結束後起實施；自111年1月25日一般交易時段結束後恢復原比例。
    如遇全日暫停交易，則恢復日順延。'''
    c=candidate_notice_clock(text,'2022-01-18')
    assert c['issue_date_bound'] and c['chronological']
    assert c['effective_date']=='2022-01-19'
    assert c['effective_phase']=='after_product_regular_close'
    assert c['requires_reversion_review'] and '2022-01-25' in c['temporary_end_evidence']
    # Reused attachment URLs cannot donate a later document to an old notice.
    assert not candidate_notice_clock(text,'2021-01-18')['issue_date_bound']
    chinese='發文日期：中華民國九十三年五月二十六日\n|上市日期|九十三年五月三十一日|'
    launch=candidate_notice_clock(chinese,'2004-05-27')
    assert launch['effective_date']=='2004-05-31'
    assert launch['effective_phase']=='new_contract_listing' and launch['issue_date_bound']


def test_position_limit_tightening_uses_its_later_effective_date():
    text='''股票期貨暨選擇權契約交易人部位限制標準調整一覽表
    部位限制數之提高，自2026/07/16起生效。
    部位限制數之降低，自2026/09/17起生效。'''
    grid=[['提高部位限制數之契約',None,None,None,None,None,None],
          ['契約名稱','契約代碼','標的證券代號','調整前部位限制級距','調整後部位限制級距','單位：契約數',None],
          [None,None,None,None,None,'自然人','法人'],
          ['富邦金期貨及選擇權','CE','2881','2','1','8,000','24,000'],
          ['降低部位限制數之契約',None,None,None,None,None,None],
          ['全新期貨','GU','2455','2','3','2,000','6,000'],
          ['全新選擇權','GU','2455','2','3','2,000','6,000']]
    facts=position_grid_candidates(grid,text)
    assert [(f['product'],f['natural_person_limit'],f['effective_date']) for f in facts]==[
        ('CEF',8000,'2026-07-16'),('GUF',2000,'2026-09-17')]
    assert all(f['unit']=='contracts' and 'before_limit' not in f for f in facts)
    assert position_grid_candidates(grid,text.replace('2026/09/17','115年9月17日'))==facts
    # Grade 2 is not evidence for its historical natural-person contract cap.
    assert facts[1]['before_grade']=='2'
    ambiguous=text+'部位限制數之降低，自2026/10/01起生效。'
    assert position_grid_candidates(grid,ambiguous)[1]['effective_date'] is None


def test_corporate_adjustment_distinguishes_equity_credit_from_deliverable_cash():
    grid=[['契約代號','不調整（仍為CDF）'],
          ['調整契約月份','115年9月、10月、12月、116年3月及6月到期契約'],
          ['約定標的物','不調整（仍為2,000股標的證券）'],
          ['契約乘數','不調整（仍為2,000）'],
          ['買方權益數加項','每口買方未沖銷部位調整買方權益數加項新臺幣14,000元'],
          ['賣方權益數減項','每口賣方未沖銷部位調整賣方權益數減項新臺幣14,000元']]
    fact=corporate_grid_candidates(grid,'調整生效日：115年9月16日','115年9月15日17:25開盤之盤後交易時段生效')[0]
    assert fact['contract_months']==['202609','202610','202612','202703','202706']
    assert fact['effective_date']=='2026-09-16'
    assert fact['equity_credit_long_per_contract']==14000
    assert fact['cash_equity_pair_agrees'] and not fact['includes_deliverable_cash']
    assert fact['requires_phase_and_condition_review'] and '17:25' in fact['phase_evidence']
    legacy=[['契約代號','DLF調整為DL1'],['契約乘數','DL1契約乘數調整為1,600。'],
            ['約定標的物','1,600股標的證券及現金新臺幣4,000元'],['調整契約月份','100年1月及2月到期契約']]
    old=corporate_grid_candidates(legacy,'調整生效日：100年1月7日')[0]
    assert old['includes_deliverable_cash'] and old['equity_credit_long_per_contract'] is None
    assert old['contract_multiplier']==1600
    assert not corporate_grid_candidates([['契約代號','CDO調整為CDA'],*grid[1:]])
    bad=[row[:] for row in grid];bad[-1][1]=bad[-1][1].replace('14,000','13,000')
    assert corporate_grid_candidates(bad)[0]['equity_credit_long_per_contract'] is None


def test_corporate_split_table_requires_named_immediate_continuation():
    pages=[dict(page=3,native_text='調整生效日：112年12月8日',tables=[
        dict(caption='2. CL1契約調整',cells=[['調整契約月份','112年12月'],
            ['契約代號','CL1調整為CL2'],['約定標的物','CL2為2,016股標的證券及現金增資相當價值'],
            ['優先參與現金增資相當價值計算方式','到期契約以41.9877股計算，若']])]),
        dict(page=4,native_text='',tables=[dict(caption='',cells=[['','不足則不予計算。'],
            ['契約乘數','CL2履約價格乘數不調整（仍為2,016）']])])]
    row,=corporate_native_table_candidates(pages)
    assert row['product']=='CL2' and row['contract_multiplier']==2016
    assert row['contract_months']==['202312']
    assert row['table_segments']==[[3,0],[4,0]]
    assert row['requires_rights_valuation']
    pages[1]['tables'][0]['cells'][1][1]='CLA履約價格乘數不調整（仍為2,000）'
    assert not corporate_native_table_candidates(pages)
    pages[1]['tables'][0]['cells'][1][1]='CL2履約價格乘數不調整（仍為2,016）'
    pages[1]['tables'][0]['caption']='二、加掛標準契約'
    assert not corporate_native_table_candidates(pages)


def test_corporate_month_scope_uses_only_its_table_or_caption():
    grid=[['契約代號','JWF調整為JW1'],['契約乘數','調整為2,100'],
          ['約定標的物','調整為2,100股標的證券']]
    caption='調整契約月份：104年9月、10月、12月、105年3月及6月到期契約。'
    body='其他契約調整契約月份：104年8月到期契約。調整生效日：104年9月14日。'
    fact=corporate_grid_candidates(grid,body,caption)[0]
    assert fact['contract_months']==['201509','201510','201512','201603','201606']
    assert fact['contract_months_origin']=='same_table_caption'
    assert corporate_grid_candidates(grid,body,'')[0]['contract_months']==[]
    assert corporate_grid_candidates(grid,body,caption+'調整月份：104年8月到期契約。')[0]['contract_months']==[]
    explicit=grid+[['調整契約月份','104年10月到期契約']]
    assert corporate_grid_candidates(explicit,body,caption)[0]['contract_months']==['201510']
    footnoted=grid+[['調整契約月份註1','104年10月到期契約']]
    fact=corporate_grid_candidates(footnoted,body,caption)[0]
    assert fact['contract_months']==['201510'] and fact['contract_months_footnote_reference']=='註1'
    common='二、調整月份：104年10月到期契約。三、調整內容：標準型與小型契約。'
    fact=corporate_grid_candidates(grid,body,'小型契約',page_text=common)[0]
    assert fact['contract_months']==['201510'] and fact['contract_months_origin']=='same_page_single_scope'
    assert corporate_grid_candidates(grid,body,'',page_text=common+'調整月份：104年12月到期契約。')[0]['contract_months']==[]
    truncated_suffix='二、調整月份註1：114年4月、6月、9月及12月\n三、調整內容：每口5,013元。註1：114年3月20日為114年5月到期契約交易開始日。'
    fact=corporate_grid_candidates(grid,body,truncated_suffix)[0]
    assert fact['contract_months']==['202504','202506','202509','202512']
    assert fact['contract_months_text']=='114年4月、6月、9月及12月'
    assert corporate_grid_candidates(grid,body,'契約月份註：114年4月到期契約')[0]['contract_months']==['202504']
    assert corporate_grid_candidates(grid,body,'契約月份：114月3月到期契約')[0]['contract_months']==[]


def test_review_includes_direct_pdf_notices_and_preserves_parent():
    c = sqlite3.connect(':memory:'); c.row_factory = sqlite3.Row
    c.executescript('''CREATE TABLE documents(url, state, parsing_status, content_sha256);
        CREATE TABLE announcements(url,published_date,title,category);
        CREATE TABLE links(parent,child);
        INSERT INTO documents VALUES('old.pdf','complete','pending_ocr','a'),
                                    ('new.pdf','complete','parsed','b');
        INSERT INTO announcements VALUES('old.pdf','2010-01-19','launch','margins'),
            ('html','2025-04-17','adjust','margins');
        INSERT INTO links VALUES('html','new.pdf');''')
    docs = review_documents(c, ['margins'])
    assert set(docs) == {'a', 'b'}
    assert docs['a']['announcements'][0]['url'] == 'old.pdf'
    assert docs['b']['announcements'][0]['url'] == 'html'
    assert set(review_documents(c, ['margins'], True)) == {'a'}


def test_official_expiry_extends_last_print_and_separates_relisted_code():
    dates = [date(2010,5,14),date(2010,5,17),date(2010,6,24),date(2010,6,25),date(2010,9,15)]
    observed = pl.DataFrame(dict(date=[dates[0],dates[2],dates[3]],
        product=['DJF']*3,contract=['201009']*3))
    final = pl.DataFrame(dict(product=['DJF']*2,contract=['201009']*2,
        settlement_date=[dates[1],dates[4]],final_settlement_price=[49.55,230.5],
        final_settlement_value=[None,None]),schema_overrides={'final_settlement_value':pl.Float64})
    calendar,lives = physical_lifetime_calendar(observed,final,pl.DataFrame({'date':dates}))
    assert lives.height == 2
    assert lives['last_observed_date'].to_list() == [dates[0],dates[3]]
    assert lives['calendar_end'].to_list() == [dates[1],dates[4]]
    assert calendar['date'].to_list() == dates
    assert calendar['physical_instance'].n_unique() == 2
    assert calendar.filter(pl.col('date')==dates[1])['final_settlement_price'].item() == 49.55
    assert calendar.filter(pl.col('date')==dates[4])['final_settlement_price'].item() == 230.5
    with pytest.raises(ValueError,match='duplicate official'):
        physical_lifetime_calendar(observed,pl.concat([final,final.head(1)]),pl.DataFrame({'date':dates}))


def test_unknown_expiry_does_not_become_a_legal_close():
    observed = pl.DataFrame(dict(date=[date(2026,9,1),date(2026,9,4)],
        product=['OLD','NEW'],contract=['202608','202609']))
    final = pl.DataFrame(schema={'product':pl.String,'contract':pl.String,
        'settlement_date':pl.Date,'final_settlement_price':pl.Float64,'final_settlement_value':pl.Float64})
    _,lives = physical_lifetime_calendar(observed,final,observed.select('date'))
    status = dict(lives.select('product','lifetime_status').iter_rows())
    assert status == {'OLD':'unresolved_expiry','NEW':'observed_at_dataset_boundary'}


def test_legacy_shared_corporate_table_binds_old_months_new_units_and_option_scope():
    cells = [
        ['調整生效日', '100 年 1 月 7 日'],
        ['調整契約月份', 'DLO 及 DLF：100 年1月、2月、3月、6月及9月到期契約。\nDL1：100年3月及6月到期契約。'],
        ['調整契約代號', 'DLO 調整為 DLA\nDLF 調整為 DL1\nDL1 調整為 DL2'],
        ['調整約定標的物', 'DLA 及 DL1 約定標的物為1,600股標的證券及新臺幣4,000元。\nDL2約定標的物為1,600股標的證券及新臺幣12,120元。'],
        ['契約乘數', 'DL1及DL2契約乘數調整為1,600。\nDLA履約價格乘數不調整，仍為2,000。'],
    ]
    result = corporate_grid_candidates(cells)
    assert [(r['from_product'],r['to_product'],r['contract_multiplier']) for r in result] == [
        ('DLF','DL1',1600.), ('DL1','DL2',1600.)]
    assert result[0]['contract_months'] == ['201101','201102','201103','201106','201109']
    assert result[1]['contract_months'] == ['201103','201106']
    assert all(r['effective_date']=='2011-01-07' and r['includes_deliverable_cash'] for r in result)
    assert '12,120' not in result[0]['deliverable_text']
    assert '4,000' not in result[1]['deliverable_text']
    assert all(r['equity_credit_long_per_contract'] is None for r in result)
    # Removing the futures clause cannot silently borrow the option's 2,000.
    cells[-1][1] = 'DLA履約價格乘數不調整，仍為2,000。'
    assert corporate_grid_candidates(cells) == []


def test_corporate_text_does_not_borrow_new_standard_or_repeated_multiplier():
    text = ('調整生效日100年1月7日調整契約代號DL1調整為DL2'
            '約定標的物1,600股及現金契約乘數字跡不清。'
            '四、推出標準契約：契約代號DLF約定標的物2,000股契約乘數調整為2,000。')
    assert corporate_text_candidates(text) == []


def test_corporate_identity_calendar_separates_reissued_same_month_without_false_expiry():
    obs=pl.DataFrame(dict(date=[date(2013,2,15),date(2013,2,19),date(2013,2,19),date(2013,3,20),date(2013,3,20)],
        product=['CNF','CNF','CN1','CNF','CN1'],contract=['201303']*5))
    final=pl.DataFrame(dict(product=['CNF','CN1'],contract=['201303']*2,
        settlement_date=[date(2013,3,20)]*2,final_settlement_price=[20.,20.],final_settlement_value=[40000.,40000.]))
    corp=pl.DataFrame([dict(product='CN1',from_product='CNF',contract_months=['201303'],
        effective_date='2013-02-19',source_content_sha256='a'*64)])
    calendar,lives=physical_lifetime_calendar(obs,final,obs.select('date'),corporate=corp)
    base=lives.filter(pl.col('product')=='CNF').sort('first_observed_date')
    assert base.height==2 and base['physical_instance'].n_unique()==2
    old,new=base.to_dicts()
    assert old['lifetime_status']=='corporate_transfer_candidate' and old['official_expiry'] is None
    assert old['corporate_transfer_target']=='CN1' and old['calendar_end']==date(2013,2,18)
    assert new['first_observed_date']==date(2013,2,19) and new['official_expiry']==date(2013,3,20)
    assert not calendar.select('date','physical_contract').is_duplicated().any()
    assert calendar['corporate_identity_candidate_only'].all()
    assert set(calendar['physical_contract'])=={'CNF:201303','CN1:201303'}
    with pytest.raises(ValueError,match='conflicting corporate identity'):
        physical_lifetime_calendar(obs,final,obs.select('date'),corporate=pl.concat([
            corp,corp.with_columns(pl.lit('CN2').alias('product'))]))


def test_raw_calendar_preserves_adjusted_codes_currency_and_explicit_universe():
    raw=pl.DataFrame(dict(date=[date(2011,1,26)]*5,
        product=['DLF','DL1','DL2','MSF','BRF'],contract=['201103']*5,session=['一般']*5))
    universe=pl.DataFrame(dict(product=['DLF','DL1','DL2','MSF'],
        asset_class=['stock_future']*3+['index_future']))
    out=raw_physical_observations(raw,universe)
    assert set(out['product'])==set(universe['product'])
    assert 'contract_multiplier' not in out.columns and 'cash_currency' not in out.columns
    assert out['source_row_observed'].all()
    with pytest.raises(ValueError,match='duplicate normalized'):
        raw_physical_observations(pl.concat([raw,raw.head(1)]),universe)


@pytest.mark.parametrize('product',['DL1','DL2'])
def test_stock_context_training_adapter_rejects_legacy_adjusted_units(tmp_path,product):
    from downloader.artifact_io import sha256_file
    from stockagent.data.tw_futures_portfolio_daily import (
        FUTURES_MODEL_FEATURE_COLUMNS,TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
        TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
    )
    from stockagent.data.tw_stock_context_futures_portfolio import attach_stock_context_futures_portfolio_daily
    row={name:0. for name in FUTURES_MODEL_FEATURE_COLUMNS}
    row.update(date=date(2011,1,26),product=product,symbol='TAIFEX_SLOT_0001',tenor_rank=1,
        open=88.,close=88.2,volume=4,holding_log_return=0.,executable=True,must_liquidate=False,
        can_hold_overnight=True,same_contract_as_previous_session=True,contract_multiplier=2000.,
        sinopac_network_fee_group='stock',underlying_symbol='2412',contract='201103',
        physical_contract=product+':201103',asset_class='stock_future',previous_volume=4.)
    path=tmp_path/'continuous_daily.parquet';pl.DataFrame([row]).write_parquet(path)
    path.with_name('manifest.json').write_text(json.dumps(dict(
        contract_version=TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
        feature_contract_version=TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
        fixed_model_output_slots=TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
        outputs={'continuous_daily':{'sha256':sha256_file(path)}})))
    panel=SimpleNamespace(dates=np.array(['2011-01-26'],dtype='datetime64[D]'))
    with pytest.raises(ValueError,match='dated adjusted deliverables'):
        attach_stock_context_futures_portfolio_daily(panel,path,
            fee_per_side_twd_by_group={'stock':40.},integer_contracts=True)
