"""Temporal rule binding cannot manufacture executable margin history."""
from datetime import date
import hashlib
import json

import polars as pl
import pytest

from stockagent.data.tw_futures_margin_preparation import align_product_margin_intervals


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
    conflicting,issues=position_candidate_intervals([dict(absolute,natural_person_limit=2001),combined])
    assert conflicting==[] and any('conflicting' in r['reasons'] for r in issues)


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


@pytest.mark.parametrize('failure',[None,'halt','extension','intervening_event','missing_source'])
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
    obs=pl.DataFrame(dict(date=days,symbol=['1795']*len(days),volume=[100.]*len(days),source_sha256=['c'*64]*len(days)))
    disposition=pl.DataFrame(ds,schema_overrides={'source_sha256':pl.String})
    if failure=='missing_source':
        with pytest.raises(ValueError,match='source identities'):
            disposal_margin_restorations(facts,u,disposition,obs)
        return
    rows,issues=disposal_margin_restorations(facts,u,disposition,obs)
    if failure:
        assert rows==[] and len(issues)==1
    else:
        assert len(rows)==1 and not issues
        assert rows[0]['after']==[.162,.1242,.12]
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
