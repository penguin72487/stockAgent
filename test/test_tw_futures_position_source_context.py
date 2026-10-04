from copy import deepcopy
from datetime import date
import json
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import polars as pl
import pytest
import scripts.build_tw_futures_margin_event_candidates as builder

from scripts.build_tw_futures_margin_event_candidates import (
    next_nearby_position_date, repair_position_source_context, retained_document_text_views,
)
from downloader.artifact_io import sha256_file


@pytest.mark.parametrize('edge',['left','right','both','partial','interior','ragged'])
def test_position_grid_removes_only_completely_empty_edge_columns(edge):
    from stockagent.data.tw_futures_margin_preparation import corporate_position_table_candidates
    cells=[['適用期間','自115.7.16起至115.9.16止'],
           ['自然人','8,400,800股'],['法人機構','25,202,400股'],['造市者','63,006,000股']]
    if edge in ('left','both','partial','ragged'):cells=[[None,*row] for row in cells]
    if edge in ('right','both'):cells=[[*row,''] for row in cells]
    if edge=='partial':cells[0][0]='另一欄'
    if edge=='interior':cells=[[row[0],'',*row[1:]] for row in cells]
    if edge=='ragged':cells[-1]=cells[-1][1:]
    original=deepcopy(cells)
    page=dict(page=2,native_text='HH1與HHF部位合併計算。',tables=[
        dict(cells=[['持有部位','HH1','HHF'],['每口折算股數','2,100.2','2,000']]),
        dict(cells=cells)])
    own=dict(product='HH1',from_product='HHF',effective_date='2026-07-16',contract_multiplier=2100.2)
    rows=corporate_position_table_candidates([page],corporate=[own])
    assert page['tables'][1]['cells']==original
    if edge in ('partial','ragged'):assert not rows
    else:
        assert {r['product'] for r in rows}=={'HH1','HHF'}
        assert all(r['natural_person_limit']==8400800 and r['effective_date']=='2026-07-16' for r in rows)
        if edge=='interior':assert all('position_empty_edge_columns' not in r for r in rows)
        else:assert all(json.loads(r['position_empty_edge_columns'])['original_cells']==original for r in rows)


@pytest.mark.parametrize('problem',[None,'missing_category','category_first','extra_number',
    'missing_unit','different_unit','unlabelled_row','duplicate_category','interleaved'])
def test_complete_vertical_text_rows_preserve_both_natural_person_amounts(problem):
    from stockagent.data.tw_futures_margin_preparation import corporate_position_flat_table_candidates
    text=('三、部位限制：\n持有部位\nAAO\nAAA\nAAF\nAA1\n每口折算股數\n'
          '2,000\n2,100\n2,000\n2,100\n(二)部位合併計算：AA1與AAF部位合併計算。\n'
          '(三)部位限制數：\n適用期間\n自107.07.26起至107.09.19止\n'
          '自107.09.20起至AA1契約終止掛牌前一營業日止\n自然人\n'
          '4,400,000股\n4,000,000股\n法人機構\n13,200,000股\n12,000,000股\n'
          '造市者\n33,000,000股\n30,000,000股\n'
          '註：部位限制數應依本契約最新適用部位限制級數計算。')
    if problem=='missing_category':text=text.replace('造市者\n33,000,000股\n30,000,000股\n','')
    if problem=='category_first':text=text.replace('自然人\n','自然人\n法人機構\n造市者\n').replace('股\n法人機構\n','股\n').replace('股\n造市者\n','股\n')
    if problem=='extra_number':text=text.replace('4,000,000股\n','4,000,000股\n1,000股\n')
    if problem=='missing_unit':text=text.replace('4,400,000股','4,400,000')
    if problem=='different_unit':text=text.replace('30,000,000股','30,000,000口')
    if problem=='unlabelled_row':text=text.replace('自然人\n','不明列\n')
    if problem=='duplicate_category':text+='\n自然人\n'
    if problem=='interleaved':text=text.replace('4,400,000股\n4,000,000股','4,400,000股\n法人機構\n4,000,000股')
    own=dict(product='AA1',from_product='AAF',effective_date='2018-07-26',
             contract_multiplier=2100.,issue_date_bound=True)
    rows=corporate_position_flat_table_candidates(text,[own])
    if problem:assert not rows
    else:
        assert {(r['product'],r['effective_date'],r['natural_person_limit']) for r in rows}=={
            (c,day,amount) for c in ('AAF','AA1') for day,amount in
            [('2018-07-26',4400000.),('2018-09-20',4000000.)]}
        assert all(r['limit_follows_applicable_grade'] for r in rows)


@pytest.mark.parametrize('printed,day,end',[
    ('100.0729','2011-07-29','100.09.21'),
    ('102.0827','2013-08-27','102.10.16'),
])
def test_printed_compact_mmdd_period_start_uses_its_own_adjustment_date(printed,day,end):
    from stockagent.data.tw_futures_margin_preparation import corporate_position_table_candidates
    period=f'自{printed}起至{end}止'
    page=dict(page=2,native_text='AAF與AA1部位合併計算。',tables=[
        dict(cells=[['持有部位','AAF','AA1'],['每口折算股數','2,000','2,100']]),
        dict(cells=[['適用期間',period],['自然人','2,100,000股']])])
    own=dict(product='AA1',from_product='AAF',effective_date=day,contract_multiplier=2100.)
    rows=corporate_position_table_candidates([page],corporate=[own])
    assert {r['product'] for r in rows}=={'AAF','AA1'}
    assert all(r['effective_date']==day and r['source_period_text']==period for r in rows)
    for row in rows:
        assert json.loads(row['position_compact_date_evidence'])==dict(
            notation='ROC_year.MMDD',literal_start=printed,same_original_adjustment_date=day)
    assert corporate_position_table_candidates([page],corporate=[dict(own,effective_date='2010-01-01')])==[]
    assert corporate_position_table_candidates([page],corporate=[own,dict(own,effective_date=None)])==[]


@pytest.mark.parametrize('printed',['100.729','100.07299','100.0732','100.1329','100.0000','100.0729.07'])
def test_compact_period_date_does_not_repair_missing_or_invalid_digits(printed):
    from stockagent.data.tw_futures_margin_preparation import corporate_position_table_candidates
    page=dict(page=2,native_text='AAF與AA1部位合併計算。',tables=[
        dict(cells=[['持有部位','AAF','AA1'],['每口折算股數','2,000','2,100']]),
        dict(cells=[['適用期間',f'自{printed}起至100.09.21止'],['自然人','2,100,000股']])])
    own=dict(product='AA1',from_product='AAF',effective_date='2011-07-29',contract_multiplier=2100.)
    assert corporate_position_table_candidates([page],corporate=[own])==[]


@pytest.mark.parametrize('same_cap',[False,True])
def test_inclusive_corporate_period_boundary_does_not_invent_an_opening_cap(same_cap):
    from stockagent.data.tw_futures_margin_preparation import (
        corporate_position_table_candidates,position_candidate_intervals,bind_dated_position_combinations,
    )
    second=8238694 if same_cap else 8000000
    page=dict(page=3,native_text='HS1、HS2與HSF部位合併計算。',tables=[
        dict(cells=[['持有部位','HS1','HS2','HSF'],
                    ['每口折算股數','2,059.6734','2,059.6734','2,000']]),
        dict(cells=[['適用期間','自108.09.05起至108.10.16止',
                    '自108.10.16起至HS1、HS2契約終止掛牌前一營業日止'],
                    ['自然人','8,238,694股',f'{second:,}股']])])
    financial=[dict(product=c,from_product=old,effective_date='2019-09-05',
                    contract_multiplier=2059.6734) for c,old in [('HS1','HSF'),('HS2','HS1')]]
    facts=corporate_position_table_candidates([page],corporate=financial)
    assert len(facts)==6
    provenance=dict(issue_date_bound=True,source_content_sha256='a'*64,source_url='own_notice',
                    published_date='2019-08-26',known_at='2019-08-26T23:59:59+08:00')
    intervals,issues=position_candidate_intervals([dict(f,**provenance) for f in facts])
    assert not issues
    days=pl.DataFrame({'date':[date(2019,10,15),date(2019,10,16),date(2019,10,17)],
                      'product':['HSF']*3})
    bound=bind_dated_position_combinations(days,pl.DataFrame(intervals,
        schema_overrides={'admission_not_before':pl.String}))
    assert bound['position_numeric_inputs_resolved'].to_list()==[True,same_cap,True]
    assert bound['position_limit'].to_list()==[8238694.,float(second) if same_cap else None,float(second)]
    later=[f for f in facts if f['effective_date']=='2019-10-16']
    assert all(f['source_period_text'].startswith('自108.10.16') for f in later)
    assert all(bool(f['position_period_overlap_evidence']) is not same_cap for f in later)


@pytest.mark.parametrize('conflict',[False,True])
def test_inspected_multi_origin_position_units_do_not_create_financial_events(conflict):
    from stockagent.data.tw_futures_margin_preparation import (
        corporate_native_table_candidates,corporate_position_table_candidates,
    )
    pages=[dict(page=2,native_text=(
        'CN1、CN2註與CNF期貨部位合併計算。'
        '若CN1契約於契約調整生效日前下市，則HZF契約代號調整為CN1而非CN2。CNF與CN1部位合併計算。'),tables=[
        dict(cells=[['調整生效日','104年10月2日'],
                    ['調整契約月份','104年10月、12月、105年3月及6月到期契約'],
                    ['契約代號','HZF調整為CN2註'],
                    ['約定標的物','調整為3,225.8股中國信託金融控股股份有限公司普通股股票'],
                    ['契約乘數','CN2註契約乘數調整為3,225.8']]),
        dict(cells=[['持有部位','CNF','CN1','CN2註'],
                    ['每口折算股數','2,000','2,162','3,225.8']]),
        dict(cells=[['適用期間','自104.10.2起至104.11.18止'],
                    ['自然人','25,806,400股'],['法人機構','77,419,200股'],
                    ['造市者','193,548,000股']])])]
    financial=corporate_native_table_candidates(pages)
    assert len(financial)==1 and financial[0]['product']=='CN2'
    assert financial[0]['from_product']=='HZF' and financial[0]['has_conditional_language']
    assert financial[0]['contract_multiplier']==3225.8
    assert not corporate_position_table_candidates(pages,corporate=financial)
    assert not corporate_position_table_candidates(pages,corporate=financial,allow_table_only_members=True)
    if conflict:financial.append(dict(product='CN1',from_product='CNF',contract_multiplier=2163))
    positions=corporate_position_table_candidates(pages,corporate=financial,
        allow_table_only_members=True,group_text='CN1、CN2註與CNF期貨部位合併計算。')
    if conflict:
        assert not positions
    else:
        assert {r['product']:r['position_unit'] for r in positions}=={
            'CN1':2162,'CN2':3225.8,'CNF':2000}
        assert all(r['combined_products']==['CN1','CN2','CNF'] for r in positions)
        assert len(financial)==1 and financial[0]['product']=='CN2'
    with pytest.raises(ValueError,match='absent'):
        corporate_position_table_candidates(pages,corporate=financial,allow_table_only_members=True,
            group_text='CN1與KIF部位合併計算。')


def test_retained_visual_position_grids_reuse_verified_reader_and_preserve_missing_cash(tmp_path,monkeypatch):
    from stockagent.data.tw_futures_margin_preparation import corporate_grid_candidates
    url='official';content='a'*64
    cells=[['調整生效日','104年10月2日'],['調整契約月份','104年10月到期契約'],
        ['契約代號','HZF調整為CN2'],['約定標的物','調整為3,225.8股標的證券'],
        ['契約乘數','CN2契約乘數調整為3,225.8']]
    own=corporate_grid_candidates(cells)[0]
    own.update(source_url=url,announcement_url=url,source_content_sha256=content,
        published_date='2015-09-14',known_at='2015-09-14T23:59:59+08:00',issue_date_bound=True)
    original=deepcopy(own)
    path=tmp_path/'review.json';path.write_text(json.dumps(dict(reviews=[dict(source_url=url)])))
    proof=dict(path=path.name,sha256=sha256_file(path),url='',kind='visual_corporate_cell_review')
    archive=SimpleNamespace(bundle=tmp_path,sources={path.name:proof},document=lambda u:
        dict(content_sha256=content,text='',format='pdf'))
    called=[]
    position=dict(product='CN1',effective_date='2015-10-02',position_unit=2162,
        natural_person_limit=25806400,source_url=url,source_content_sha256=content,
        extraction_method='native_cell_grid')
    def verified_reader(a,p,*,source_urls=None):
        assert a is archive and p==path and source_urls=={url}
        called.append(p)
        return [own],[position],{content}
    monkeypatch.setattr(builder,'corporate_source_review_candidates',verified_reader)
    result=builder.retained_corporate_position_text_candidates(archive,[own],source_urls={url})
    assert len(result)==1 and result[0]['product']=='CN1' and result[0]['position_unit']==2162
    assert result[0]['extraction'].startswith('retained_corporate_position_table:visual_corporate_cell_review:')
    assert called==[path] and own==original and own['equity_credit_long_per_contract'] is None
    path.write_text('{}')
    with pytest.raises(ValueError,match='review SHA mismatch'):
        builder.retained_corporate_position_text_candidates(archive,[own],source_urls={url})


@pytest.mark.parametrize('review_kind',['visual_corporate_cell_review','visual_position_cell_review'])
@pytest.mark.parametrize('problem',[None,'unreviewed','different_quantity','missing_unchanged_clause'])
def test_retained_standard_review_joins_literal_unchanged_member_without_repairing_text(tmp_path,monkeypatch,problem,review_kind):
    url='official';content='a'*64
    provenance=dict(source_url=url,announcement_url=url,source_content_sha256=content,
        published_date='2020-01-20',known_at='2020-01-20T23:59:59+08:00',issue_date_bound=True)
    first=dict(provenance,product='AA1',from_product='AAF',effective_date='2020-02-03',
        contract_multiplier=2000.,deliverable_security_quantity=2000.,contract_months=['202006'])
    second=dict(provenance,product='AA2',from_product='AA1',effective_date='2020-02-03',
        contract_multiplier=2100.,deliverable_security_quantity=2100.,contract_months=['202006'],
        subscription_rights_at_final_settlement=True)
    if problem=='different_quantity':first['contract_multiplier']=2200.
    own=[first,second];original=deepcopy(own)
    text=('加掛標準契約：契約代號AAF約定標的物2.000股標的證券。'
        '部位限制：AAF、AA1與AA2部位合併計算。調整約定標的物2,100股標的證券。'
        'AA2契約乘數不調整（仍為2,100）')
    if problem=='missing_unchanged_clause':text=text.split('AA2契約乘數')[0]
    path=tmp_path/'review.json';path.write_text(json.dumps(dict(reviews=[dict(source_url=url,
        review_kind='source_bound_visual_corporate_position_group')])))
    source=dict(path=path.name,sha256=sha256_file(path),url='',kind=review_kind)
    archive=SimpleNamespace(bundle=tmp_path,sources={} if problem=='unreviewed' else {path.name:source},
        document=lambda u:dict(content_sha256=content,text=text,format='pdf'))
    standard=dict(provenance,product='AA1',effective_date='2020-02-03',unit='contracts',
        event_type='combined_position_formula',combined_position_base_product='AAF',
        combined_position_ratio='1/1',combined_position_evidence='AAF、AA1與AA2部位合併計算',
        extraction_method='source_bound_visual_corporate_position_group',visual_review_sha256='b'*64)
    called=[]
    def verified(a,p,*,source_urls=None):
        assert a is archive and p==path and source_urls=={url}
        called.append(p);return own,[standard],{content}
    monkeypatch.setattr(builder,'corporate_source_review_candidates',verified)
    def verified_position(a,p,financial,*,source_urls=None):
        assert financial==own
        return verified(a,p,source_urls=source_urls)[1]
    monkeypatch.setattr(builder,'position_source_review_candidates',verified_position)
    rows=builder.retained_corporate_position_text_candidates(archive,own,source_urls={url})
    formulas=[r for r in rows if r.get('event_type')=='combined_securities_position_formula']
    if problem:assert not formulas
    else:
        (formula,)=formulas
        assert formula['product']=='AA2' and formula['combined_position_ratio']=='21/20'
        assert formula['natural_person_limit'] is None
        assert formula['source_content_sha256']==content
        assert called==[path] and '2.000股' in text
    assert own==original


@pytest.mark.parametrize('problem',[None,'foreign_view','changed_view','conflicting_group','missing_cap'])
def test_same_original_retained_group_text_recovers_only_grid_owned_position_cells(tmp_path,problem):
    from stockagent.data.tw_futures_margin_preparation import corporate_grid_candidates
    url='official';content='a'*64
    own=corporate_grid_candidates([
        ['調整生效日','104年10月2日'],['調整契約月份','104年10月到期契約'],
        ['契約代號','AAF調整為AA1'],['約定標的物','2,100股標的證券'],
        ['契約乘數','AA1契約乘數調整為2,100']])[0]
    own.update(source_url=url,announcement_url=url,source_content_sha256=content,
        published_date='2015-09-14',known_at='2015-09-14T23:59:59+08:00',issue_date_bound=True)
    original=deepcopy(own)
    pages=[dict(page=2,native_text='AAF與AB1部位合併計算。' if problem=='conflicting_group' else '',tables=[
        dict(cells=[['持有部位','AAF','AA1'],['每口折算股數','2,000','2,100']]),
        dict(cells=[['適用期間','自104.10.2起至104.11.18止'],
                    ['自然人',None if problem=='missing_cap' else '4,200,000股']])])]
    table=tmp_path/'tables.json';table.write_text(json.dumps(dict(pages=pages)))
    receipt=tmp_path/'receipt.json';receipt.write_text(json.dumps(dict(status='complete',
        content_sha256=content,files=[dict(path='tables.json',sha256=sha256_file(table))])))
    text=tmp_path/'text.txt';text.write_text('AA1與AAF部位合併計算。'
        '部位限制數應依本契約最新適用部位限制級數計算。自然人999,000,000股。')
    text_receipt=tmp_path/'text_receipt.json';text_receipt.write_text(json.dumps(dict(
        status='complete',content_sha256=content,
        files=[dict(path='candidate.txt',sha256=sha256_file(text))])))
    sources={p.name:dict(path=p.name,sha256=sha256_file(p),url=url,kind=k)
        for p,k in [(table,'ocr_table_tables.json'),(receipt,'ocr_table_receipt.json'),
                     (text,'review_pages_including_ocr'),(text_receipt,'page_extraction_receipt')]}
    if problem=='foreign_view':sources[text.name]['url']='other_original'
    if problem=='changed_view':text.write_text('different bytes')
    archive=SimpleNamespace(bundle=tmp_path,sources=sources,
        document=lambda u:dict(content_sha256=content,text='',format='pdf'))
    if problem=='changed_view':
        with pytest.raises(ValueError,match='SHA mismatch'):
            builder.retained_corporate_position_text_candidates(archive,[own],source_urls={url})
        return
    rows=builder.retained_corporate_position_text_candidates(archive,[own],source_urls={url})
    if problem:
        assert not rows
    else:
        assert {r['product'] for r in rows}=={'AAF','AA1'}
        assert all(r['natural_person_limit']==4200000. and r['effective_date']=='2015-10-02'
                   and r['limit_follows_applicable_grade'] and r['known_at']==own['known_at'] for r in rows)
        assert {r['position_unit'] for r in rows}=={2000.,2100.}
        assert all('review_pages_including_ocr' in r['position_group_text_views'] for r in rows)
    assert own==original



@pytest.mark.parametrize('role',['corporate','position','wrong_source','changed_bytes'])
def test_relocated_financial_review_binds_the_same_original_page_across_roles(tmp_path,role):
    image=tmp_path/'page.png';image.write_bytes(b'one original page used by two reviewed scopes')
    digest=sha256_file(image)
    cells=[['契約代號','AAF調整為AA1'],['調整生效日','104年10月14日'],
        ['調整契約月份','104年11月到期契約'],
        ['約定標的物','2,000股標的證券及現金100元'],
        ['契約乘數','AA1契約乘數調整為2,000']]
    review=dict(review_kind='source_bound_visual_corporate_cells',source_url='official',
        content_sha256='a'*64,published_date='2015-10-02',visually_reviewed_pages=[2],pages=[
            dict(page=2,image_path='old_missing_folder/page.png',image_sha256=digest,
                native_text='發文日期：中華民國104年10月2日。調整生效日104年10月14日。',
                tables=[dict(cells=cells)])])
    path=tmp_path/'review.json';path.write_text(json.dumps(dict(reviews=[review])))
    class Archive:
        bundle=tmp_path
        conn=SimpleNamespace(execute=lambda *args:SimpleNamespace(fetchone=lambda:
            {'published_date':'2015-10-02'}))
        sources={'page':dict(path=image.name,sha256=digest,
            kind='visual_corporate_review_page' if role=='corporate' else 'visual_position_review_page',
            url='foreign' if role=='wrong_source' else 'official')}
        def document(self,url):return dict(content_sha256='a'*64)
        def copy(self,p,h,**kwargs):
            if sha256_file(p)!=h:raise ValueError('archive source hash mismatch')
    if role=='changed_bytes':image.write_bytes(b'changed copied image')
    if role in ('wrong_source','changed_bytes'):
        with pytest.raises(ValueError,match='bound page receipt|image SHA mismatch'):
            builder.corporate_source_review_candidates(Archive(),path)
    else:
        financial,positions,replaced=builder.corporate_source_review_candidates(Archive(),path)
        assert len(financial)==1 and financial[0]['product']=='AA1'
        assert financial[0]['deliverable_cash_twd']==100 and not positions and replaced=={'a'*64}


@pytest.mark.parametrize('problem',[None,'missing_date','ambiguous_date','conflicting_unit',
                                   'extra_category','extra_member','explicit_period'])
def test_headerless_single_member_cap_requires_complete_owned_grid(problem):
    from stockagent.data.tw_futures_margin_preparation import corporate_position_table_candidates
    pages=[dict(page=2,native_text='二、部位限制：註：部位限制數應依本契約最新適用部位限制級數計算。',
        tables=[dict(cells=[['持有部位','HW1'],['每口折算股數','2,034']]),
                dict(cells=[['自然人','4,068,000股'],['法人機構','12,204,000股'],
                            ['造市者','30,510,000股']])])]
    financial=[dict(from_product='HWF',product='HW1',effective_date='2016-08-24',
                    contract_multiplier=2034.)]
    if problem=='missing_date':financial[0]['effective_date']=None
    if problem=='ambiguous_date':financial.append(dict(financial[0],effective_date='2016-08-25'))
    if problem=='conflicting_unit':pages[0]['tables'][1]['cells'][0][1]='4,068,000受益權單位'
    if problem=='extra_category':pages[0]['tables'][1]['cells'].append(['其他','1股'])
    if problem=='extra_member':
        pages[0]['tables'][0]['cells'][0].append('HWF');pages[0]['tables'][0]['cells'][1].append('2,000')
    if problem=='explicit_period':pages[0]['native_text']+='適用期間：自105.8.24起至105.9.21止。'
    rows=corporate_position_table_candidates(pages,corporate=financial)
    if problem:
        assert not rows
    else:
        row,=rows
        assert row['product']=='HW1' and row['combined_products']==['HW1']
        assert row['position_unit']==2034 and row['natural_person_limit']==4068000
        assert row['effective_date']=='2016-08-24' and row['end_rule']=='until_superseding_rule'


def test_long_overlapping_restoration_subjects_have_a_bounded_parse():
    # Each coded entry can also be read as a named alias. The unmatched final
    # subject used to trigger exponential backtracking on retained notices.
    code = """
from scripts.build_tw_futures_margin_event_candidates import named_margin_restoration_clauses
header='NDF(南亞期貨)'
subjects='、'.join(['南亞期貨(NDF)']*40)
tail='於115年1月22日一般交易時段結束後恢復為115年1月5日調整前保證金。'
good=named_margin_restoration_clauses(header+'。'+subjects+tail)
assert len(good)==1 and good[0]['products']=={'NDF'}
assert named_margin_restoration_clauses(header+'。'+subjects+'及未命名商品'+tail) is None
"""
    subprocess.run([sys.executable, '-c', code], check=True, timeout=5)


@pytest.mark.parametrize('problem',[None,'wrong_amount','option_code','two_dates',
    'past_listing','wrong_issue','wrong_source','monthly_cap'])
def test_native_futures_listing_clock_does_not_borrow_immediate_option_clock(tmp_path,problem):
    text=('發文日期：中華民國99年1月14日\n'
          '主旨：公告本公司99年1月25日新上市35檔「股票期貨契約」及新增股票選擇權。\n'
          '公告事項：調整股票選擇權部位限制，並自即日起實施。\n'
          '附件1 股票期貨部位限制契約數\n'
          '|股票期貨英文代碼|契約名稱|級數|自然人|法人|\n'
          '|CRF|遠東新期貨|3|2,500|7,500|\n')
    row=dict(product='CRF',source_url='native',source_content_sha256='a'*64,
        published_date='2010-01-15',known_at='2010-01-15T23:59:59+08:00',
        issue_date_bound=True,effective_date=None,effective_phase='unresolved',
        direction='absolute',unit='contracts',natural_person_limit=2500,
        notice_revoked_effective_date='2010-01-22',notice_revocation_source_sha256='b'*64)
    if problem=='wrong_amount':row['natural_person_limit']=1250
    if problem=='option_code':row['product']='CRO'
    if problem=='two_dates':text+='99年2月1日新上市1檔「股票期貨契約」\n'
    if problem=='past_listing':text=text.replace('99年1月25日','99年1月13日')
    if problem=='wrong_issue':text=text.replace('99年1月14日','98年1月14日')
    if problem=='monthly_cap':row['natural_person_monthly_limit']=100
    if problem=='wrong_source':row['source_content_sha256']='c'*64
    a=SimpleNamespace(bundle=tmp_path,sources={},
        document=lambda url:dict(content_sha256='a'*64,text=text))
    original=deepcopy(row)
    if problem=='wrong_source':
        with pytest.raises(ValueError,match='candidate bytes'):
            repair_position_source_context(a,[row],pl.DataFrame(),[])
        assert row==original
    elif problem:
        assert not repair_position_source_context(a,[row],pl.DataFrame(),[])
        assert row==original
    else:
        changes=repair_position_source_context(a,[row],pl.DataFrame(),[])
        assert len(changes)==1 and changes[0]['kind']=='exact_native_futures_listing_row'
        assert row['effective_date']=='2010-01-25' and row['effective_phase']=='new_contract_listing'
        for key in ('known_at','natural_person_limit','notice_revoked_effective_date',
                    'notice_revocation_source_sha256','source_content_sha256'):
            assert row[key]==original[key]
        assert not repair_position_source_context(a,[row],pl.DataFrame(),[])


def lives():
    return pl.DataFrame([
        dict(product='HSF', contract=month, first_observed_date=date(2019, 12, 1),
             last_observed_date=end, official_expiry=end, calendar_end=end)
        for month, end in [('202001', date(2020, 1, 15)), ('202002', date(2020, 2, 19)),
                           ('202003', date(2020, 3, 18)), ('202006', date(2020, 6, 17))]
    ])


def test_next_nearby_uses_post_publication_month_and_next_actual_session():
    result = next_nearby_position_date('HSF', '2020-01-15', lives(),
                                       [date(2020, 3, 18), date(2020, 3, 19)])
    assert result['next_nearby_contract'] == '202003'
    assert result['effective_date'] == '2020-03-19'
    # A null near expiry is not permission to skip to a more distant month.
    broken = lives().with_columns(pl.when(pl.col('contract') == '202002')
        .then(None).otherwise(pl.col('official_expiry')).alias('official_expiry'))
    assert next_nearby_position_date('HSF', '2020-01-15', broken, [date(2020, 3, 19)]) is None
    assert next_nearby_position_date('HSF', '2020-01-15', lives(), [date(2020, 3, 18)]) is None
    duplicates = pl.concat([lives(), lives().filter(pl.col('contract') == '202002')])
    assert next_nearby_position_date('HSF', '2020-01-15', duplicates, [date(2020, 3, 19)]) is None


def split_near_lives():
    rows=[]
    for row in lives().to_dicts():
        row.update(asset_class='stock_future',lifetime_status='official_final',
                   final_fixing_origin='official_product_final',corporate_boundary=None,
                   corporate_transfer_target=None,corporate_boundary_sources=[])
        if row['contract'] in ('202002','202003'):
            future=dict(row,first_observed_date=date(2020,2,11))
            rows.append(future)
            row.update(official_expiry=None,last_observed_date=date(2020,2,9),
                calendar_end=date(2020,2,9),corporate_boundary=date(2020,2,10),
                corporate_transfer_target='HS1',corporate_boundary_sources=['a'*64],
                lifetime_status='corporate_transfer_candidate',final_fixing_origin=None)
        rows.append(row)
    return pl.DataFrame(rows)


def test_next_nearby_split_instance_uses_exact_nominal_calendar_only():
    result=next_nearby_position_date('HSF','2020-01-15',split_near_lives(),[date(2020,3,19)])
    assert result['next_nearby_contract']=='202003'
    assert result['next_nearby_expiry']=='2020-03-18'
    assert result['effective_date']=='2020-03-19'
    proofs=result['nominal_month_calendar_proofs']
    assert {p['contract'] for p in proofs}=={'202002','202003'}
    assert all(p['scope']=='same_product_and_already_listed_nominal_month_calendar_only' for p in proofs)
    assert not any('price' in k or 'multiplier' in k for p in proofs for k in p)


@pytest.mark.parametrize('problem',['missing_calendar','wrong_product','wrong_month','conflicting_date',
    'duplicate_active','missing_transfer_source','wrong_transfer_sha','missing_target','non_stock',
    'expiry_before_transfer','unlisted_old_month','nonofficial_final','null_final_origin','null_final_status'])
def test_next_nearby_split_calendar_rejects_unproved_identities(problem):
    frame=split_near_lives();rows=frame.to_dicts()
    old=next(r for r in rows if r['contract']=='202002' and r['official_expiry'] is None)
    final=next(r for r in rows if r['contract']=='202002' and r['official_expiry'] is not None)
    if problem=='missing_calendar':rows.remove(final)
    elif problem=='wrong_product':final['product']='CJF'
    elif problem=='wrong_month':final['contract']='202012'
    elif problem=='conflicting_date':rows.append(dict(final,official_expiry=date(2020,2,20)))
    elif problem=='duplicate_active':rows.append(dict(old))
    elif problem=='missing_transfer_source':old['corporate_boundary_sources']=[]
    elif problem=='wrong_transfer_sha':old['corporate_boundary_sources']=['not-a-sha']
    elif problem=='missing_target':old['corporate_transfer_target']=None
    elif problem=='non_stock':old['asset_class']='index_future'
    elif problem=='expiry_before_transfer':old['corporate_boundary']=date(2020,2,20)
    elif problem=='unlisted_old_month':old['first_observed_date']=date(2020,1,16)
    elif problem=='nonofficial_final':final['final_fixing_origin']='estimated'
    elif problem=='null_final_origin':final['final_fixing_origin']=None
    elif problem=='null_final_status':final['lifetime_status']=None
    assert next_nearby_position_date('HSF','2020-01-15',pl.DataFrame(rows),[date(2020,3,19)]) is None


def archive(tmp_path, docs, covers=()):
    conn = sqlite3.connect(':memory:')
    conn.executescript('CREATE TABLE announcements(url TEXT,published_date TEXT);'
                       'CREATE TABLE links(parent TEXT,child TEXT);'
                       'CREATE TABLE documents(url TEXT,state TEXT);')
    conn.execute('INSERT INTO announcements VALUES (?,?)', ('notice', '2021-01-20'))
    for url in ['annex', *covers]:
        conn.execute('INSERT INTO links VALUES (?,?)', ('notice', url))
    for url in docs:
        conn.execute('INSERT INTO documents VALUES (?,?)', (url, 'complete'))
    return SimpleNamespace(conn=conn, bundle=tmp_path, sources={},
        document=lambda url: dict(text=docs[url], content_sha256=url + '-sha'),
        children=lambda url: [r[0] for r in conn.execute('SELECT child FROM links WHERE parent=?', (url,))])


def fact():
    return dict(product='HSF', source_url='annex', source_content_sha256='annex-sha',
        published_date='2021-01-20', issue_date_bound=False, effective_date='2021-01-21',
        effective_phase='product_regular_open', direction='raise', after=8000)


COVER = ('發文日期：中華民國110年1月20日 股票期貨 部位限制 詳如附件 '
         '調高者，自110年1月21日起生效；調降者，自110年3月18日起生效。')


def test_multiple_retained_ocr_views_bind_each_own_receipt(tmp_path):
    sources = []
    for index, text in enumerate(['first extraction 16.20%', 'second extraction 13.50%']):
        page = tmp_path / f'view{index}.txt'
        page.write_text(text)
        receipt = tmp_path / f'proof{index}.json'
        receipt.write_text(json.dumps(dict(status='complete', content_sha256='source-sha',
            files=[dict(path='candidate.txt', sha256=sha256_file(page))])))
        for path, kind in [(page, 'review_pages_including_ocr'), (receipt, 'page_extraction_receipt')]:
            sources.append(dict(url='source', path=path.name, sha256=sha256_file(path), kind=kind))
    a = SimpleNamespace(bundle=tmp_path, sources={s['path']: s for s in sources})
    document = dict(text='native', content_sha256='source-sha')
    views = retained_document_text_views(a, 'source', document, sources=sources)
    assert {t for t, _ in views} == {'native', 'first extraction 16.20%', 'second extraction 13.50%'}
    assert retained_document_text_views(a, 'source', document) == views
    with pytest.raises(ValueError, match='matching extraction receipt'):
        retained_document_text_views(a, 'source', document, sources=sources[:-1])
    (tmp_path / 'view0.txt').write_text('tampered')
    with pytest.raises(ValueError, match='text SHA mismatch'):
        retained_document_text_views(a, 'source', document, sources=sources)


def test_reextraction_preserves_source_bound_visual_cells_only_in_reviewed_scope():
    row = dict(source_content_sha256='a' * 64, source_url='notice', product='CYF',
        margin_kind='notional_rate', before=None, after=[.135, .1035, .1])
    reviewed = dict(row, extraction='source_bound_visual_transcription')
    flawed = dict(row, after=[.162, .1242, .12])
    sibling = dict(flawed, product='CWF')
    other_notice = dict(flawed, source_content_sha256='b' * 64)
    result = builder.retain_margin_code_corrections([reviewed], [flawed, sibling, other_notice])
    assert result == [sibling, other_notice]


def test_margin_restoration_dates_follow_product_clauses_not_other_revocations():
    text = ('配合證券市場處置期間，115年1月22日一般交易時段結束後，南亞科期貨契約(CYF)、'
            '金居期貨契約(PQF)及力積電期貨契約(QZF)恢復為115年1月12日調整前保證金，'
            '晶豪科期貨契約(IIF)恢復為115年1月7日調整前保證金；'
            '華邦電期貨契約(FZF)於115年1月26日一般交易時段結束後恢復為115年1月12日調整前保證金。'
            '原115年1月6日公告自115年1月12日晶豪科期貨一般交易時段結束後停止適用。')
    base = dict(effective_date='2026-01-12', effective_phase='after_product_regular_close',
                requires_reversion_review=True, temporary_end_evidence='[]')
    for product, day in [('CYF', '2026-01-22'), ('PQF', '2026-01-22'),
                         ('IIF', '2026-01-22'), ('FZF', '2026-01-26')]:
        result = builder.product_margin_restoration_clock(text, product, base)
        assert result['effective_date'] == '2026-01-12'
        assert {e['date_iso'] for e in json.loads(result['temporary_end_evidence'])} == {day}
    assert builder.product_margin_restoration_clock(text, 'TX', base) == base
    ambiguous = text + '另115年1月23日一般交易時段結束後恢復原保證金。'
    assert builder.product_margin_restoration_clock(ambiguous, 'CYF', base) == base


def test_short_restoration_reference_excludes_only_bound_numbered_legal_citation():
    row=dict(product='NFF',effective_date='2016-01-15',known_at='2016-01-14T23:59:59+08:00',
        issue_date_bound=True,before=[.135,.1035,.1])
    text=('中華民國105年1月14日。依104年7月31日台期監字10410007970號函「保證金處置調整措施」，'
          '調高漢微科期貨(NFF)，自105年1月15日起實施，於1月27日恢復為1月15日調整前之保證金。')
    result=builder.referenced_before_restoration([text],row,[row])
    assert result==dict(restoration_target=row['before'],restoration_rule='return_to_declared_before')
    assert builder.referenced_before_restoration([text],dict(row,issue_date_bound=False),[row]) is None
    for bad in [text.replace('台期監字10410007970號函「保證金處置調整措施」','其他日期'),
                text+'另104年12月1日為調整生效日。',text.replace('恢復為1月15日','恢復為1月18日')]:
        assert builder.referenced_before_restoration([bad],row,[row]) is None


@pytest.mark.parametrize('clause',[
    '並自104年8月21日該股票期貨契約交易時段結束後，開始實施。',
    '並自該標的證券處置生效日次一營業日(104年8月21日)該股票期貨契約交易時段結束後，開始實施。',
])
def test_named_stock_close_keeps_exact_date_and_requires_session_proof(clause):
    masthead='臺灣期貨交易所新聞稿 中華民國104年8月20日 '
    result=builder.candidate_notice_clock(masthead+clause,'2015-08-20')
    assert result['issue_date_bound'] and result['effective_date']=='2015-08-21'
    assert result['effective_phase']=='after_product_trading_session_unspecified'
    # The dated single-session proof is applied separately. A later era does
    # not gain a regular-session boundary from this generic closing phrase.
    later=builder.candidate_notice_clock((masthead+clause).replace('104年','110年'),'2021-08-20')
    assert later['effective_phase']=='after_product_trading_session_unspecified'
    missing=builder.candidate_notice_clock(masthead+
        '並自該標的證券處置生效日次一營業日該股票期貨契約交易時段結束後，開始實施。','2015-08-20')
    assert missing['effective_date'] is None
    conflict=builder.candidate_notice_clock(masthead+clause+
        '另自104年8月24日該股票期貨契約交易時段結束後，開始實施。','2015-08-20')
    assert conflict['effective_date'] is None


def test_visual_corporate_identity_replaces_only_reviewed_origin_date_months():
    reviewed = dict(source_content_sha256='a'*64, from_product='IAF', product='IA1',
        effective_date='2020-07-22', contract_months=['202008', '202009'])
    wrong = dict(reviewed, product='TA1')
    others = [dict(wrong, contract_months=['202012']), dict(wrong, from_product='TQF'),
              dict(wrong, source_content_sha256='b'*64),
              dict(wrong, effective_date='2021-07-22'), dict(wrong, contract_months=[])]
    result = builder.replace_reviewed_corporate_facts([wrong, *others], [reviewed])
    assert result == [*others, reviewed]


def test_corporate_position_extraction_uses_independent_verified_units(tmp_path):
    text=('加掛標準型股票期貨契約：約定標的物2,000股標的證券。'
          '部位限制：AA1與AAF部位合併計算。')
    digest='a'*64
    row=dict(product='AA1',from_product='AAF',effective_date='2020-01-01',
        contract_months=['202006'],contract_multiplier=2000.,deliverable_security_quantity=2000.,
        source_url='original',source_content_sha256=digest,issue_date_bound=True,
        published_date='2019-12-20',known_at='2019-12-20T23:59:59+08:00',
        deliverable_components_resolved=False,has_equity_credit_fields=True,cash_equity_pair_agrees=False)
    a=SimpleNamespace(bundle=tmp_path,sources={},
        document=lambda url:dict(text=text,content_sha256=digest))
    result=builder.retained_corporate_position_text_candidates(a,[row])
    assert len(result)==1 and result[0]['product']=='AA1'
    assert result[0]['combined_position_base_product']=='AAF'
    assert result[0]['combined_position_ratio']=='1/1'
    assert result[0]['source_content_sha256']==digest
    assert result[0]['point_in_time_verified'] is False
    assert not row['deliverable_components_resolved']
    for change in [dict(deliverable_security_quantity=None),dict(issue_date_bound=False),
                   dict(known_at='2020-01-02T23:59:59+08:00')]:
        assert not builder.retained_corporate_position_text_candidates(a,[dict(row,**change)])
    conflict=dict(row,contract_multiplier=2100.,deliverable_security_quantity=2100.)
    assert not builder.retained_corporate_position_text_candidates(a,[row,conflict])
    a.document=lambda url:dict(text=text,content_sha256='b'*64)
    with pytest.raises(ValueError,match='source identity drift'):
        builder.retained_corporate_position_text_candidates(a,[row])


@pytest.mark.parametrize('problem', [None,'wrong_standard','missing_clause','changed_unit',
    'conflicting_standard','numeric_fragment','unbound_clock','borrow_cover_unit'])
def test_two_code_retained_group_scopes_cover_mentions_and_annex_units(tmp_path,problem):
    text=('發文日期：中華民國109年10月27日。調整內容、加掛標準契約及部位限制等，詳列附件。'
          '二、加掛標準契約：上市日109年11月5日契約代號AAF'
          '約定標的物2,000股標的證券。三、部位限制：AA1與AAF部位合併計算。')
    digest='a'*64
    row=dict(product='AA1',from_product='AAF',effective_date='2020-11-05',
        contract_months=['202011','202012'],contract_multiplier=2000.,deliverable_security_quantity=2000.,
        source_url='original',source_content_sha256=digest,issue_date_bound=True,
        published_date='2020-10-27',known_at='2020-10-27T23:59:59+08:00')
    if problem=='wrong_standard':text=text.replace('契約代號AAF','契約代號ABF')
    if problem=='missing_clause':text=text.replace('AA1與AAF部位合併計算','限制未載')
    if problem=='changed_unit':row.update(contract_multiplier=2060.,deliverable_security_quantity=2060.)
    if problem=='conflicting_standard':
        text+='二、加掛標準契約：契約代號AAF約定標的物100股標的證券。三、部位限制：AA1與AAF部位合併計算。'
    if problem=='numeric_fragment':text+='自然人4,000,000股部位限制數缺頁。'
    if problem=='unbound_clock':row['issue_date_bound']=False
    if problem=='borrow_cover_unit':
        text=text.replace('加掛標準契約及部位限制','加掛標準契約約定標的物2,000股標的證券及部位限制')
        text=text.replace('契約代號AAF約定標的物2,000股標的證券','契約代號AAF約定標的物缺值')
    a=SimpleNamespace(bundle=tmp_path,sources={},document=lambda url:dict(text=text,content_sha256=digest))
    result=builder.retained_corporate_position_text_candidates(a,[row])
    if problem is not None:
        assert result==[]
    else:
        assert len(result)==1
        assert result[0]['product']=='AA1' and result[0]['effective_date']=='2020-11-05'
        assert result[0]['combined_position_base_product']=='AAF'
        assert result[0]['combined_position_ratio']=='1/1'
        assert result[0]['natural_person_limit'] is None
        assert result[0]['known_at']==row['known_at'] and result[0]['source_content_sha256']==digest


def test_position_relative_day_start_uses_same_observed_next_nearby_contract(tmp_path):
    clause=('發文日期：中華民國109年7月14日 調整本公司部位限制。'
        '調降者，自公告日該期貨或該選擇權已上市之次近月份契約到期後次一營業日起生效。')
    observed=pl.DataFrame([dict(product='HSF',contract=month,
        first_observed_date=date(2020,6,1),last_observed_date=end,official_expiry=end,calendar_end=end)
        for month,end in [('202007',date(2020,7,15)),('202008',date(2020,8,19)),
                         ('202009',date(2020,9,16))]])
    row=dict(product='HSF',source_url='annex',source_content_sha256='annex-sha',
        published_date='2020-07-14',issue_date_bound=True,effective_date=None,
        effective_phase='product_regular_open',direction='lower',natural_person_limit=2000.)
    a=archive(tmp_path,{'annex':clause})
    rows=[deepcopy(row)]
    changes=builder.repair_position_source_context(a,rows,observed,[date(2020,8,19),date(2020,8,20)])
    assert len(changes)==1 and rows[0]['effective_date']=='2020-08-20'
    assert rows[0]['natural_person_limit']==row['natural_person_limit']
    for bad in [clause.replace('次近月份','第三近月份'),clause+'惟除HSF另行公告。']:
        a.document=lambda url:dict(text=bad,content_sha256='annex-sha')
        unchanged=[deepcopy(row)]
        assert not builder.repair_position_source_context(a,unchanged,observed,[date(2020,8,20)])
        assert unchanged==[row]


def test_legacy_absolute_position_table_inherits_only_explicit_listing_context(tmp_path):
    text='發文日期：中華民國99年5月17日。前揭契約訂於99年5月24日上市。DSF宏碁期貨自然人2,500口。'
    row=dict(product='DSF',source_url='annex',source_content_sha256='annex-sha',
        published_date='2010-05-17',issue_date_bound=True,effective_date=None,
        effective_phase='unresolved',direction='absolute',unit='contracts',natural_person_limit=2500.)
    a=archive(tmp_path,{'annex':text})
    rows=[deepcopy(row)]
    changes=builder.repair_position_source_context(a,rows,pl.DataFrame(),[])
    assert len(changes)==1 and rows[0]['effective_date']=='2010-05-24'
    assert rows[0]['effective_phase']=='new_contract_listing'
    assert rows[0]['natural_person_limit']==row['natural_person_limit']
    for bad in [text.replace('上市','起生效'),text.replace('DSF','DWF'),
                text.replace('99年5月17日','99年5月1日'),text+'另訂於99年5月25日上市。']:
        a.document=lambda url:dict(text=bad,content_sha256='annex-sha')
        unchanged=[deepcopy(row)]
        assert not builder.repair_position_source_context(a,unchanged,pl.DataFrame(),[])
        assert unchanged==[row]


def test_same_day_grade_change_keeps_combined_share_constraint_and_source_chain(tmp_path):
    old = dict(product='EYF', effective_date='2021-06-17', effective_phase='product_regular_open',
        event_type='absolute_level', unit='contracts', natural_person_limit=8000,
        known_at='2021-04-20T23:59:59+08:00', issue_date_bound=True,
        source_content_sha256='a'*64, source_url='old')
    new = dict(old, effective_date='2021-12-16', natural_person_limit=4000,
        known_at='2021-10-20T23:59:59+08:00', source_content_sha256='b'*64, source_url='new')
    corporate = dict(old, unit='shares', event_type='corporate_securities_unit_limit',
        effective_date='2021-12-16', natural_person_limit=16000000, position_unit=2000,
        combined_products=['EYF','EY1'], source_content_sha256='c'*64, source_url='corporate',
        known_at='2021-10-18T23:59:59+08:00')
    a = SimpleNamespace(bundle=tmp_path, sources={}, document=lambda url:dict(
        content_sha256='c'*64,text='部位限制數應依本契約最新適用部位限制級數計算'))
    simultaneous = [dict(old), dict(new, known_at=corporate['known_at']), dict(corporate),
                    dict(corporate, product='EY1', position_unit=2080)]
    assert len(builder.repair_same_day_position_grade(a, simultaneous)) == 1
    assert simultaneous[2]['natural_person_limit'] == 8000000
    rows=[old,new,corporate,dict(corporate,product='EY1',position_unit=2080)]
    result=builder.repair_same_day_position_grade(a,rows)
    assert len(result)==1 and result[0]['corrected_share_limit']==8000000
    assert all(r['natural_person_limit']==8000000 for r in rows[2:])
    assert all(r['position_grade_source_sha256s']==['a'*64,'b'*64,'c'*64] for r in rows[2:])
    intervals,issues=builder.position_candidate_intervals(rows)
    selected=next(r for r in intervals if r['product']=='EYF' and r['effective_date']=='2021-12-16')
    assert selected['unit']=='shares' and selected['combined_products']==['EY1','EYF']
    assert selected['position_limit']==8000000 and selected['known_at']==new['known_at']
    assert selected['source_content_sha256s']==['a'*64,'b'*64,'c'*64]
    assert not issues
    # Nullable evidence columns survive a Parquet round trip on unrelated facts.
    reloaded = [dict(old, position_grade_source_sha256s=None),
                dict(new, position_grade_source_sha256s=None), *rows[2:]]
    restored, errors = builder.position_candidate_intervals(reloaded)
    assert not errors
    assert restored == intervals
    # A rule unknown before the legal boundary cannot supply a new cap.
    for known in ['2021-12-16T12:00:00+08:00']:
        unchanged=[old,dict(new,known_at=known),dict(corporate,natural_person_limit=16000000)]
        assert not builder.repair_same_day_position_grade(a,unchanged)
    # An enlarged temporary cap needs its own formula, not this restored-basis case.
    assert not builder.repair_same_day_position_grade(a,
        [old,new,dict(corporate,natural_person_limit=16640000)])
    no_clause=SimpleNamespace(bundle=tmp_path,sources={},document=lambda url:dict(content_sha256='c'*64,text='部位限制'))
    assert not builder.repair_same_day_position_grade(no_clause,
        [old,new,dict(corporate,natural_person_limit=16000000)])


def initial_period_grade_case(tmp_path, grade_known='2014-07-17T23:59:59+08:00'):
    old = dict(product='DDF', effective_date='2013-02-20', effective_phase='product_regular_open',
        event_type='absolute_level', unit='contracts', natural_person_limit=8000.,
        known_at='2013-02-20T23:59:59+08:00', issue_date_bound=True,
        source_content_sha256='a'*64, source_url='prior_grade')
    new = dict(old, effective_date='2014-09-18', natural_person_limit=4000.,
        known_at=grade_known, source_content_sha256='b'*64, source_url='active_grade')
    standard = dict(old, effective_date='2014-11-20', unit='shares',
        event_type='corporate_securities_unit_limit', natural_person_limit=16000000.,
        position_unit=2000., combined_products=['DD1', 'DDF'],
        known_at='2014-09-17T23:59:59+08:00', source_content_sha256='c'*64,
        source_url='corporate', requires_delisting_clock=True)
    adjusted = dict(standard, product='DD1', position_unit=2108.4675)
    docs = dict(corporate=dict(content_sha256='c'*64,
        text='部位限制數應依本契約最新適用部位限制級數計算'))
    a = SimpleNamespace(bundle=tmp_path, sources={}, document=lambda url: docs[url])
    return a, [old, new, standard, adjusted], docs


@pytest.mark.parametrize('grade_known', ['2014-07-17T23:59:59+08:00', '2014-10-05T23:59:59+08:00'])
def test_active_grade_before_second_period_uses_own_start_without_backdating(tmp_path, grade_known):
    a, rows, _ = initial_period_grade_case(tmp_path, grade_known)
    before = deepcopy(rows)
    repairs = builder.repair_same_day_position_grade(a, rows)
    assert len(repairs) == 1
    assert repairs[0]['effective_date'] == '2014-11-20'
    assert repairs[0]['grade_effective_date'] == '2014-09-18'
    assert repairs[0]['grade_start_contract'] == 'dated_active_grade_at_period_start_v2'
    assert rows[:2] == before[:2]
    assert [r['position_unit'] for r in rows[2:]] == [2000., 2108.4675]
    assert all(r['natural_person_limit'] == 8000000. for r in rows[2:])
    assert all(r['known_at'] == max(grade_known, '2014-09-17T23:59:59+08:00') for r in rows[2:])
    assert all(r['effective_date'] == '2014-11-20' for r in rows[2:])
    assert all(r['position_grade_source_sha256s'] == ['a'*64, 'b'*64, 'c'*64] for r in rows[2:])
    assert not builder.position_candidate_intervals(rows)[1]
    repaired = deepcopy(rows)
    assert not builder.repair_same_day_position_grade(a, rows)
    assert rows == repaired


def test_original_future_grade_basis_excludes_later_unpublished_end_boundary(tmp_path):
    a,rows,docs=active_grade_case(tmp_path)
    docs['corporate']['text']='部位限制數應依本契約最新適用部位限制級數計算。'
    rows[1]['known_at']='2019-12-20T23:59:59+08:00'
    for row in rows[3:]:
        row['effective_date']='2020-02-10';row['natural_person_limit']=1000000
    replacement=dict(rows[1],effective_date='2020-02-05',natural_person_limit=600,
        known_at='2020-01-20T23:59:59+08:00',source_url='replacement',source_content_sha256='e'*64)
    docs['replacement']=dict(content_sha256='e'*64,text=docs['quarter1']['text'])
    rows.append(replacement)
    repairs=builder.repair_same_day_position_grade(a,rows)
    assert len(repairs)==1 and repairs[0]['corrected_share_limit']==1200000
    assert repairs[0]['basis_scope']=='announced_grade_at_original_period_start'
    assert repairs[0]['basis_original_known_at']=='2020-01-02T23:59:59+08:00'
    assert all(r['known_at']=='2020-01-20T23:59:59+08:00' for r in rows[3:5])
    assert all(r['effective_date']=='2020-02-10' for r in rows[3:5])
    changes=builder.repair_active_position_grade_boundaries(a,rows)
    assert len(changes)==1 and changes[0]['basis_contract_limit']==500
    assert changes[0]['new_contract_limit']==750 and changes[0]['corrected_share_limit']==1500000
    assert changes[0]['effective_date']=='2020-03-01'


@pytest.mark.parametrize('problem',['known_supersession','explicit_end','late_actual_grade','no_clause','enlarged'])
def test_announced_grade_basis_preserves_real_ends_and_unproved_period_starts(tmp_path,problem):
    a,rows,docs=active_grade_case(tmp_path)
    docs['corporate']['text']='部位限制數應依本契約最新適用部位限制級數計算。'
    rows[1]['known_at']='2019-12-20T23:59:59+08:00'
    for row in rows[3:]:row.update(effective_date='2020-02-10',natural_person_limit=1000000)
    replacement=dict(rows[1],effective_date='2020-02-05',natural_person_limit=600,
        known_at='2020-01-20T23:59:59+08:00',source_url='replacement',source_content_sha256='e'*64)
    docs['replacement']=dict(content_sha256='e'*64,text=docs['quarter1']['text'])
    if problem=='known_supersession':replacement['known_at']='2019-12-21T23:59:59+08:00'
    elif problem=='explicit_end':rows[1]['valid_until_date_inclusive']='2020-02-09'
    elif problem=='late_actual_grade':replacement['known_at']='2020-02-10T23:59:59+08:00'
    elif problem=='no_clause':docs['corporate']['text']='固定股數，不隨最新級數調整。'
    else:
        for row in rows[3:]:row['natural_person_limit']=1050000
    rows.append(replacement);original=deepcopy(rows)
    assert not builder.repair_same_day_position_grade(a,rows) and rows==original


@pytest.mark.parametrize('problem', ['missing_clause', 'enlarged_cap', 'incomplete_group',
    'expired_grade', 'future_grade', 'late_grade', 'monthly_cap', 'ambiguous_grade',
    'wrong_standard_units', 'same_day_close', 'same_day_unknown_phase'])
def test_active_grade_period_start_preserves_unproved_constraints(tmp_path, problem):
    a, rows, docs = initial_period_grade_case(tmp_path)
    if problem == 'missing_clause': docs['corporate']['text'] = '部位限制'
    elif problem == 'enlarged_cap':
        for row in rows[2:]: row['natural_person_limit'] = 16867740.
    elif problem == 'incomplete_group': rows.pop()
    elif problem == 'expired_grade': rows[1]['valid_until_date_inclusive'] = '2014-11-19'
    elif problem == 'future_grade': rows[1]['effective_date'] = '2014-11-21'
    elif problem == 'late_grade': rows[1]['known_at'] = '2014-11-20T00:00:00+08:00'
    elif problem == 'monthly_cap': rows[1]['natural_person_monthly_limit'] = 2000.
    elif problem == 'ambiguous_grade': rows.insert(2, dict(rows[1], natural_person_limit=5000.,
                                                         source_content_sha256='d'*64))
    elif problem == 'wrong_standard_units': rows[2]['position_unit'] = 2001.
    else:
        rows[1]['effective_date'] = '2014-11-20'
        rows[1]['effective_phase'] = ('regular_close' if problem == 'same_day_close'
                                     else 'date_only_requires_phase_review')
    original = deepcopy(rows)
    assert not builder.repair_same_day_position_grade(a, rows)
    assert rows == original


@pytest.mark.parametrize('grade_known', ['2013-07-19T23:59:59+08:00',
    '2013-08-12T23:59:59+08:00', '2013-09-01T23:59:59+08:00'])
def test_announced_future_grade_uses_effective_day_and_later_input_clock(tmp_path, grade_known):
    old = dict(product='DEF', effective_date='2013-06-20', effective_phase='product_regular_open',
        event_type='absolute_level', unit='contracts', natural_person_limit=8000.,
        known_at='2013-04-19T23:59:59+08:00', issue_date_bound=True,
        source_content_sha256='a'*64, source_url='prior_grade')
    new = dict(old, effective_date='2013-09-23', natural_person_limit=4000.,
        known_at=grade_known, source_content_sha256='b'*64, source_url='next_grade')
    corporate = dict(old, effective_date='2013-09-23', unit='shares',
        event_type='corporate_securities_unit_limit', natural_person_limit=16000000.,
        position_unit=2000., combined_products=['DEF', 'DE1'],
        known_at='2013-08-12T23:59:59+08:00', source_content_sha256='c'*64,
        source_url='corporate', requires_delisting_clock=True)
    rows = [old, new, corporate, dict(corporate, product='DE1', position_unit=2159.2844)]
    a = SimpleNamespace(bundle=tmp_path, sources={}, document=lambda url: dict(
        content_sha256='c'*64, text='部位限制數應依本契約最新適用部位限制級數計算'))
    assert len(builder.repair_same_day_position_grade(a, rows)) == 1
    combined_known = max(grade_known, '2013-08-12T23:59:59+08:00')
    assert all(r['known_at'] == combined_known for r in rows[2:])
    intervals, issues = builder.position_candidate_intervals(rows)
    assert not issues
    restored = next(r for r in intervals if r['product'] == 'DEF'
                    and r['effective_date'] == '2013-09-23')
    assert restored['position_limit'] == 8000000.
    assert restored['requires_delisting_clock']
    assert restored['known_at'] == combined_known
    assert restored['source_content_sha256s'] == ['a'*64, 'b'*64, 'c'*64]


@pytest.mark.parametrize('problem',[None,'missing_clause','missing_flag','late_publication',
    'enlarged_cap','missing_prior','monthly_cap','source_identity'])
def test_quarterly_notice_can_replace_the_adjusted_group_grade_on_its_own_source(tmp_path,problem):
    old=dict(product='EUF',effective_date='2012-06-21',effective_phase='product_regular_open',
        event_type='absolute_level',unit='contracts',natural_person_limit=1250,
        known_at='2012-04-20T23:59:59+08:00',issue_date_bound=True,
        source_content_sha256='a'*64,source_url='prior')
    new=dict(old,effective_date='2012-09-20',natural_person_limit=350,
        known_at='2012-07-24T23:59:59+08:00',source_content_sha256='b'*64,
        source_url='quarter',limit_follows_applicable_grade=True)
    corporate=dict(old,unit='shares',event_type='corporate_securities_unit_limit',
        effective_date='2012-09-20',natural_person_limit=2500000,position_unit=2000,
        combined_products=['EUF','EU1'],source_content_sha256='c'*64,source_url='corporate',
        known_at='2012-07-03T23:59:59+08:00')
    clause='本次部位限制數調整生效後，則依其最新適用之部位限制級數計算'
    if problem=='missing_clause':clause='本次部位限制數調整生效前，仍依原公告計算'
    if problem=='missing_flag':new['limit_follows_applicable_grade']=False
    if problem=='late_publication':new['known_at']='2012-09-20T12:00:00+08:00'
    if problem=='enlarged_cap':corporate['natural_person_limit']=2750000
    if problem=='monthly_cap':new['natural_person_monthly_limit']=100
    docs={'corporate':dict(content_sha256='c'*64,text='舊股數表'),
          'quarter':dict(content_sha256='d'*64 if problem=='source_identity' else 'b'*64,text=clause)}
    a=SimpleNamespace(bundle=tmp_path,sources={},document=lambda url:docs[url])
    rows=([old] if problem!='missing_prior' else [])+[new,corporate,
        dict(corporate,product='EU1',position_unit=2200)]
    before=deepcopy(rows)
    if problem=='source_identity':
        with pytest.raises(ValueError,match='source identity mismatch'):
            builder.repair_same_day_position_grade(a,rows)
    elif problem:
        assert not builder.repair_same_day_position_grade(a,rows) and rows==before
    else:
        repaired=builder.repair_same_day_position_grade(a,rows)
        assert len(repaired)==1 and repaired[0]['clause_scope']=='quarterly_notice'
        assert repaired[0]['grade_source_urls']==['quarter']
        assert all(r['natural_person_limit']==700000 for r in rows[2:])
        assert all(r['original_natural_person_limit']==2500000 for r in rows[2:])
        assert all(r['known_at']==new['known_at'] for r in rows[2:])
        assert rows[2]['position_unit']==2000 and rows[3]['position_unit']==2200
        intervals,issues=builder.position_candidate_intervals(rows)
        assert not issues
        restored=next(r for r in intervals if r['product']=='EUF' and r['effective_date']=='2012-09-20')
        assert restored['position_limit']==700000 and restored['independent_contract_limit'] is None
        assert restored['source_content_sha256s']==['a'*64,'b'*64,'c'*64]


def active_grade_case(tmp_path):
    old=dict(product='AAF',effective_date='2020-01-01',effective_phase='product_regular_open',
        event_type='absolute_level',unit='contracts',natural_person_limit=1000,
        known_at='2019-12-01T23:59:59+08:00',issue_date_bound=True,
        source_content_sha256='a'*64,source_url='prior')
    first=dict(old,effective_date='2020-02-01',natural_person_limit=500,
        known_at='2020-01-20T23:59:59+08:00',source_content_sha256='b'*64,
        source_url='quarter1',limit_follows_applicable_grade=True)
    second=dict(first,effective_date='2020-03-01',natural_person_limit=750,
        known_at='2020-02-20T23:59:59+08:00',source_content_sha256='d'*64,source_url='quarter2')
    corporate=dict(old,unit='shares',event_type='corporate_securities_unit_limit',
        effective_date='2020-01-10',natural_person_limit=2000000,position_unit=2000,
        combined_products=['AA1','AAF'],source_content_sha256='c'*64,source_url='corporate',
        known_at='2020-01-02T23:59:59+08:00',valid_until_date_inclusive='2020-03-31')
    clause='本次部位限制數調整生效後，則依其最新適用之部位限制級距計算'
    docs={'quarter1':dict(content_sha256='b'*64,text=clause),
          'quarter2':dict(content_sha256='d'*64,text=clause),
          'corporate':dict(content_sha256='c'*64,text='原調整表，無最新級數條款')}
    archive=SimpleNamespace(bundle=tmp_path,sources={},document=lambda url:docs[url])
    rows=[old,first,second,corporate,dict(corporate,product='AA1',position_unit=2100)]
    return archive,rows,docs


@pytest.mark.parametrize('problem',[None,'no_reparse','different_weight','different_group','late'])
def test_securities_member_successor_retires_only_its_reparsed_old_grade(tmp_path,monkeypatch,problem):
    a,rows,docs=active_grade_case(tmp_path)
    for row in rows[3:]:
        row['combined_products']=['AA2','AAF']
        if row['product']=='AA1':row['product']='AA2'
    originals=deepcopy(rows)
    replacement=dict(product='AA2',effective_date='2020-02-15',effective_phase='product_regular_open',
        known_at='2020-02-10T23:59:59+08:00',issue_date_bound=True,
        source_content_sha256='e'*64,source_url='new_member',unit='shares',
        event_type='combined_securities_position_formula',natural_person_limit=None,
        combined_products=['AA1','AA2','AAF'],combined_position_base_product='AAF',
        combined_position_ratio='21/20',position_group_unit_evidence='exact unchanged transfer units',
        extraction_method='explicit_unchanged_quantity_securities_combination')
    if problem=='late':replacement['known_at']='2020-02-15T23:59:59+08:00'
    docs['new_member']=dict(content_sha256='e'*64,text='AAF、AA1與AA2部位合併計算。')
    proof=deepcopy(replacement)
    if problem=='different_weight':proof['combined_position_ratio']='11/10'
    if problem=='different_group':proof['combined_products']=['AA1','AA3','AAF']
    monkeypatch.setattr(builder,'retained_corporate_position_text_candidates',
        lambda *args,**kwargs:[] if problem=='no_reparse' else [proof])
    rows.append(replacement)
    builder.repair_active_position_grade_boundaries(a,rows,corporate=[dict(own='source-bound')])
    assert rows[:5]==originals and replacement in rows
    future=[r for r in rows if r.get('extraction')=='source_composed_position_grade_boundary'
        and r['product']=='AA2' and r['effective_date']=='2020-03-01']
    assert bool(future)==(problem is not None)
    assert any(r.get('extraction')=='source_composed_position_grade_boundary'
        and r['product']=='AAF' and r['effective_date']=='2020-03-01' for r in rows)


@pytest.mark.parametrize('problem',[None,'late_grade','missing_clause','enlarged_cap',
    'uncertain_start','superseded_grade','wrong_source'])
def test_active_grade_basis_can_be_announced_before_original_publication(tmp_path,problem):
    a,rows,docs=active_grade_case(tmp_path)
    # The original notice knows the February grade in January, and prints a
    # period starting after that grade becomes effective. Its publication-day
    # grade is a different value and cannot serve as the arithmetic basis.
    docs['corporate']['text']='部位限制數應依本契約最新適用部位限制級數計算。'
    rows[1]['known_at']='2019-12-20T23:59:59+08:00'
    for row in rows[3:]:
        row['effective_date']='2020-02-10'
        row['natural_person_limit']=1000000
    if problem=='late_grade':rows[1]['known_at']='2020-01-03T23:59:59+08:00'
    if problem=='missing_clause':docs['corporate']['text']='依原公告固定股數上限計算。'
    if problem=='enlarged_cap':
        for row in rows[3:]:row['natural_person_limit']=1100000
    if problem=='uncertain_start':
        rows[1]['effective_phase']='date_only_requires_phase_review'
        for row in rows[3:]:row['effective_date']='2020-02-01'
    if problem=='superseded_grade':
        replacement=dict(rows[1],effective_date='2020-02-05',natural_person_limit=600,
            source_url='replacement',source_content_sha256='e'*64)
        docs['replacement']=dict(content_sha256='e'*64,text=docs['quarter1']['text'])
        rows.append(replacement)
    if problem=='wrong_source':docs['corporate']['content_sha256']='e'*64
    before=deepcopy(rows)
    if problem=='wrong_source':
        with pytest.raises(ValueError,match='source identity mismatch'):
            builder.repair_active_position_grade_boundaries(a,rows)
    elif problem:
        assert not builder.repair_active_position_grade_boundaries(a,rows)
    else:
        changes=builder.repair_active_position_grade_boundaries(a,rows)
        assert len(changes)==1 and changes[0]['corrected_share_limit']==1500000
        assert changes[0]['basis_scope']=='announced_grade_at_original_period_start'
        assert changes[0]['basis_date']=='2020-02-10'
        assert changes[0]['basis_contract_limit']==500
        assert changes[0]['basis_original_known_at']==before[3]['known_at']
        assert not builder.position_candidate_intervals(rows)[1]
        identical=deepcopy(rows)
        assert not builder.repair_active_position_grade_boundaries(a,rows) and rows==identical
        third=dict(rows[2],effective_date='2020-03-15',natural_person_limit=1000,
            known_at='2020-03-10T23:59:59+08:00',source_url='quarter3',source_content_sha256='f'*64)
        docs['quarter3']=dict(content_sha256='f'*64,text=docs['quarter2']['text'])
        rows.append(third)
        resumed=builder.repair_active_position_grade_boundaries(a,rows)
        assert len(resumed)==1 and resumed[0]['corrected_share_limit']==2000000
        assert resumed[0]['basis_date']=='2020-02-10'
    assert rows[:len(before)]==before


def test_active_share_period_tracks_multiple_dated_quarterly_grades_and_source_clocks(tmp_path):
    a,rows,docs=active_grade_case(tmp_path)
    originals=deepcopy(rows)
    changes=builder.repair_active_position_grade_boundaries(a,rows)
    assert len(changes)==2 and rows[:5]==originals
    assert len(rows)==9
    assert [r['corrected_share_limit'] for r in changes]==[1000000.,1500000.]
    assert all(r['combined_products']==['AA1','AAF'] for r in changes)
    added=rows[5:]
    assert [r['position_unit'] for r in added]==[2100,2000,2100,2000]
    assert all(r['valid_until_date_inclusive']=='2020-03-31' for r in added)
    assert all(r['known_at']==rows[1]['known_at'] for r in added[:2])
    assert all(r['known_at']==rows[2]['known_at'] for r in added[2:])
    assert all(not r.get('supersedes_source_sha256s') for r in added)
    intervals,issues=builder.position_candidate_intervals(rows)
    assert not issues
    values={r['effective_date']:r['position_limit'] for r in intervals if r['product']=='AA1'}
    assert values=={'2020-01-10':2000000.,'2020-02-01':1000000.,'2020-03-01':1500000.}
    same=deepcopy(rows)
    assert not builder.repair_active_position_grade_boundaries(a,rows) and rows==same
    # A later source extension can continue the proved ORIGINAL basis.
    third=dict(rows[2],effective_date='2020-03-15',natural_person_limit=1000,
        known_at='2020-03-10T23:59:59+08:00',source_url='quarter3',source_content_sha256='f'*64)
    docs['quarter3']=dict(content_sha256='f'*64,text=docs['quarter2']['text'])
    rows.append(third)
    resumed=builder.repair_active_position_grade_boundaries(a,rows)
    assert len(resumed)==1 and resumed[0]['corrected_share_limit']==2000000.


@pytest.mark.parametrize('problem',['missing_clause','missing_flag','missing_prior','missing_member',
    'enlarged_cap','ambiguous_unit','expired','late_clock','monthly','wrong_source'])
def test_active_grade_composition_rejects_unproved_or_inapplicable_boundaries(tmp_path,problem):
    a,rows,docs=active_grade_case(tmp_path)
    if problem=='missing_clause':docs['quarter1']['text']='本次調整生效前依原公告'
    if problem=='missing_flag':rows[1]['limit_follows_applicable_grade']=False
    if problem=='missing_prior':rows.pop(0)
    if problem=='missing_member':rows.pop()
    if problem=='enlarged_cap':
        for r in rows[3:]:r['natural_person_limit']=2110000
    if problem=='ambiguous_unit':rows.append(dict(rows[3],position_unit=1800))
    if problem=='expired':
        for r in rows[3:]:r['valid_until_date_inclusive']='2020-01-31'
    if problem=='late_clock':rows[1]['known_at']='2020-02-02T23:59:59+08:00'
    if problem=='monthly':rows[1]['natural_person_monthly_limit']=100
    if problem=='wrong_source':docs['quarter1']['content_sha256']='e'*64
    before=deepcopy(rows)
    if problem=='wrong_source':
        with pytest.raises(ValueError,match='source identity mismatch'):
            builder.repair_active_position_grade_boundaries(a,rows)
    elif problem in ('missing_clause','missing_flag'):
        changes=builder.repair_active_position_grade_boundaries(a,rows)
        assert len(changes)==1 and changes[0]['effective_date']=='2020-03-01'
        assert changes[0]['missing_period_not_admitted']
        assert changes[0]['reanchor_after_unproved_grade_boundaries'][0]['effective_date']=='2020-02-01'
        assert rows[:len(before)]==before
        assert all(r['effective_date']=='2020-03-01' for r in rows[len(before):])
        return
    else:
        assert not builder.repair_active_position_grade_boundaries(a,rows)
    assert rows==before


@pytest.mark.parametrize('same_day',[False,True])
def test_unchanged_active_grade_retains_new_source_and_its_opening_clock(tmp_path,same_day):
    from stockagent.data.tw_futures_margin_preparation import bind_dated_position_combinations
    a,rows,_=active_grade_case(tmp_path)
    rows[1]['natural_person_limit']=rows[0]['natural_person_limit']
    if same_day:rows[1]['known_at']='2020-02-01T23:59:59+08:00'
    before=deepcopy(rows)
    changes=builder.repair_active_position_grade_boundaries(a,rows)
    assert len(changes)==2 and changes[0]['corrected_share_limit']==2000000.
    assert rows[:len(before)]==before
    same_amount=[r for r in rows[len(before):] if r['effective_date']=='2020-02-01']
    assert {r['product'] for r in same_amount}=={'AAF','AA1'}
    assert all(r['natural_person_limit']==2000000. for r in same_amount)
    assert all('b'*64 in r['position_grade_source_sha256s'] for r in same_amount)
    assert all(r['known_at']==rows[1]['known_at'] for r in same_amount)
    levels,issues=builder.position_candidate_intervals(rows)
    assert not issues
    queries=pl.DataFrame({'date':[date(2020,2,1),date(2020,2,2)],'product':['AA1']*2})
    bound=bind_dated_position_combinations(queries,pl.DataFrame(levels,
        schema_overrides={'admission_not_before':pl.String}))
    assert bound['position_numeric_inputs_resolved'].to_list()==[not same_day,True]
    assert 'b'*64 in bound['source_content_sha256s'][1]
    identical=deepcopy(rows)
    assert not builder.repair_active_position_grade_boundaries(a,rows) and rows==identical


def test_active_grade_reentry_emits_a_boundary_even_if_unproved_amount_matches(tmp_path):
    a,rows,docs=active_grade_case(tmp_path)
    docs['quarter1']['text']='無調整型適用級数條款'
    rows[2]['natural_person_limit']=rows[1]['natural_person_limit']
    originals=deepcopy(rows)
    changes=builder.repair_active_position_grade_boundaries(a,rows)
    assert len(changes)==1 and changes[0]['effective_date']=='2020-03-01'
    assert changes[0]['corrected_share_limit']==1000000
    assert changes[0]['prior_numeric_grade_is_not_a_verified_corporate_cap']
    assert rows[:5]==originals
    assert all(r['known_at']=='2020-02-20T23:59:59+08:00' for r in rows[5:])
    assert all(r['effective_date']=='2020-03-01' for r in rows[5:])
    a2,other,other_docs=active_grade_case(tmp_path)
    other_docs['quarter1']['text']=other_docs['quarter2']['text']='無適用級數條款'
    untouched=deepcopy(other)
    assert not builder.repair_active_position_grade_boundaries(a2,other)
    assert other==untouched


def test_same_day_published_active_grade_remains_unknown_at_that_days_open(tmp_path):
    a,rows,_=active_grade_case(tmp_path)
    rows[1]['known_at']='2020-02-01T23:59:59+08:00'
    changes=builder.repair_active_position_grade_boundaries(a,rows)
    assert len(changes)==2
    intervals,issues=builder.position_candidate_intervals(rows)
    assert not issues
    schema={'admission_not_before':pl.String,'valid_until_date_exclusive':pl.String,
            'combined_position_base_product':pl.String}
    days=pl.DataFrame(dict(date=[date(2020,2,1),date(2020,2,2)],product=['AA1']*2))
    from stockagent.data.tw_futures_margin_preparation import bind_dated_position_combinations
    bound=bind_dated_position_combinations(days,pl.DataFrame(intervals,infer_schema_length=None,
        schema_overrides=schema)).sort('date')
    assert bound['position_numeric_inputs_resolved'].to_list()==[False,True]
    assert bound['position_limit'].to_list()==[None,1000000.]


def test_date_only_active_grade_preserves_the_uncertain_day_and_continues_after_it(tmp_path):
    a,rows,docs=active_grade_case(tmp_path)
    docs['corporate']['text']='部位限制數應依本契約最新適用部位限制級數計算。'
    rows[1]['limit_follows_applicable_grade']=False
    rows[1]['effective_phase']='date_only_requires_phase_review'
    changes=builder.repair_active_position_grade_boundaries(a,rows)
    assert len(changes)==2
    first=changes[0]
    assert first['grade_legal_date_only'] is True
    assert first['grade_admission_not_before']=='2020-02-01T23:59:59+08:00'
    composed=[r for r in rows if r.get('extraction')=='source_composed_position_grade_boundary'
              and r['effective_date']=='2020-02-01']
    assert len(composed)==2
    assert {r['effective_phase'] for r in composed}=={'date_only_requires_phase_review'}
    intervals,issues=builder.position_candidate_intervals(rows)
    assert not issues
    schema={'admission_not_before':pl.String,'valid_until_date_exclusive':pl.String,
            'combined_position_base_product':pl.String}
    days=pl.DataFrame(dict(date=[date(2020,2,1),date(2020,2,2)]*2,
                          product=['AA1']*2+['AAF']*2))
    from stockagent.data.tw_futures_margin_preparation import bind_dated_position_combinations
    bound=bind_dated_position_combinations(days,pl.DataFrame(intervals,infer_schema_length=None,
        schema_overrides=schema)).sort('product','date')
    assert bound['position_numeric_inputs_resolved'].to_list()==[False,True,False,True]
    assert bound['position_limit'].to_list()==[None,1000000.,None,1000000.]


@pytest.mark.parametrize('basis_day,allowed',[('2020-01-01',True),('2020-01-02',False)])
def test_date_only_prior_basis_is_usable_only_after_its_uncertain_day(tmp_path,basis_day,allowed):
    a,rows,_=active_grade_case(tmp_path)
    rows[0]['effective_date']=basis_day
    rows[0]['effective_phase']='date_only_requires_phase_review'
    changes=builder.repair_active_position_grade_boundaries(a,rows)
    assert bool(changes)==allowed
    if allowed:
        assert len(changes)==2
        assert [r['corrected_share_limit'] for r in changes]==[1000000.,1500000.]


@pytest.mark.parametrize('problem',[None,'missing_clause','wrong_corporate_source',
    'wrong_grade_source','missing_prior','missing_member','late_clock','monthly','enlarged_cap'])
def test_active_corporate_latest_grade_instruction_is_independent_of_quarterly_flag(tmp_path,problem):
    a,rows,docs=active_grade_case(tmp_path)
    docs['corporate']['text']='部位限制數應依本契約最新適用部位限制級數計算。'
    for row in rows[1:3]:row['limit_follows_applicable_grade']=False
    if problem=='missing_clause':docs['corporate']['text']='部位限制數依原公告計算。'
    if problem=='wrong_corporate_source':docs['corporate']['content_sha256']='e'*64
    if problem=='wrong_grade_source':docs['quarter1']['content_sha256']='e'*64
    if problem=='missing_prior':rows.pop(0)
    if problem=='missing_member':rows.pop()
    if problem=='late_clock':rows[1]['known_at']='2020-02-02T23:59:59+08:00'
    if problem=='monthly':rows[1]['natural_person_monthly_limit']=100
    if problem=='enlarged_cap':
        for row in rows[3:]:row['natural_person_limit']=2110000
    before=deepcopy(rows)
    if problem and problem.startswith('wrong_'):
        with pytest.raises(ValueError,match='source identity mismatch'):
            builder.repair_active_position_grade_boundaries(a,rows)
    elif problem:
        assert not builder.repair_active_position_grade_boundaries(a,rows)
    else:
        changes=builder.repair_active_position_grade_boundaries(a,rows)
        assert len(changes)==2 and rows[:5]==before
        assert all(r['clause_scope']=='corporate_notice_active_period' for r in changes)
        assert all(r['clause_source_urls']==['corporate'] for r in changes)
        assert [r['corrected_share_limit'] for r in changes]==[1000000.,1500000.]
        assert changes[1]['source_content_sha256s']==['a'*64,'b'*64,'c'*64,'d'*64]
        assert not builder.position_candidate_intervals(rows)[1]
    if problem:assert rows==before


@pytest.mark.parametrize('already_composed',[False,True])
def test_later_unchanged_member_generation_stops_only_old_member_derived_grades(tmp_path,already_composed):
    a,rows,docs=active_grade_case(tmp_path)
    originals=deepcopy(rows)
    if already_composed:
        builder.repair_active_position_grade_boundaries(a,rows)
    replacement=dict(product='AA1',effective_date='2020-02-15',effective_phase='product_regular_open',
        known_at='2020-02-10T23:59:59+08:00',issue_date_bound=True,
        source_content_sha256='e'*64,source_url='new_member',unit='contracts',
        event_type='combined_position_formula',natural_person_limit=None,
        combined_position_base_product='AAF',combined_position_ratio='1/1',
        extraction_method='explicit_unchanged_unit_corporate_combination')
    docs['new_member']=dict(content_sha256='e'*64,text='AA1與AAF部位合併計算。')
    rows.append(replacement)
    changes=builder.repair_active_position_grade_boundaries(a,rows)
    assert rows[:5]==originals and replacement in rows
    derived=[r for r in rows if r.get('extraction')=='source_composed_position_grade_boundary']
    assert {(r['product'],r['effective_date']) for r in derived}=={
        ('AA1','2020-02-01'),('AAF','2020-02-01'),('AAF','2020-03-01')}
    retired=json.loads((tmp_path/'superseded_composed_position_grades.json').read_text())
    assert len(retired)==int(already_composed)
    if retired:
        assert retired[0]['retired_row']['product']=='AA1'
        assert retired[0]['successor']['source_content_sha256']=='e'*64
    else:
        assert changes[-1]['active_products']==['AAF']
        assert changes[-1]['member_generation_boundaries']['AA1']['effective_date']=='2020-02-15'
    identical=deepcopy(rows)
    assert not builder.repair_active_position_grade_boundaries(a,rows) and rows==identical


@pytest.mark.parametrize('problem',['late','no_clause','unknown_method','wrong_ratio','wrong_source'])
def test_member_generation_end_requires_own_known_literal_successor(tmp_path,problem):
    a,rows,docs=active_grade_case(tmp_path)
    replacement=dict(product='AA1',effective_date='2020-02-15',effective_phase='product_regular_open',
        known_at='2020-02-10T23:59:59+08:00',issue_date_bound=True,
        source_content_sha256='e'*64,source_url='new_member',unit='contracts',
        event_type='combined_position_formula',natural_person_limit=None,
        combined_position_base_product='AAF',combined_position_ratio='1/1',
        extraction_method='explicit_unchanged_unit_corporate_combination')
    docs['new_member']=dict(content_sha256='e'*64,text='AA1與AAF部位合併計算。')
    if problem=='late':replacement['known_at']='2020-02-15T23:59:59+08:00'
    if problem=='no_clause':docs['new_member']['text']='未確定部位合併方式。'
    if problem=='unknown_method':replacement['extraction_method']='unverified'
    if problem=='wrong_ratio':replacement['combined_position_ratio']='2/1'
    if problem=='wrong_source':docs['new_member']['content_sha256']='f'*64
    rows.append(replacement)
    if problem=='wrong_source':
        with pytest.raises(ValueError,match='successor source identity mismatch'):
            builder.repair_active_position_grade_boundaries(a,rows)
    else:
        changes=builder.repair_active_position_grade_boundaries(a,rows)
        assert len(changes)==2 and 'active_products' not in changes[-1]
        assert any(r.get('extraction')=='source_composed_position_grade_boundary'
                   and r['product']=='AA1' and r['effective_date']=='2020-03-01' for r in rows)


@pytest.mark.parametrize('problem',[None,'missing_review','different_scope','changed_review'])
def test_reviewed_member_generation_requires_reparsed_exact_source(tmp_path,monkeypatch,problem):
    a,rows,docs=active_grade_case(tmp_path)
    path=tmp_path/'review.json';path.write_text('{"reviews": []}')
    replacement=dict(product='AA1',effective_date='2020-02-15',effective_phase='product_regular_open',
        known_at='2020-02-10T23:59:59+08:00',issue_date_bound=True,
        source_content_sha256='e'*64,source_url='new_member',unit='contracts',
        event_type='combined_position_formula',natural_person_limit=None,
        combined_position_base_product='AAF',combined_position_ratio='1/1',
        extraction_method='source_bound_visual_corporate_position_group',
        position_formula_parser='explicit_unchanged_unit_corporate_combination',
        visual_review_sha256=sha256_file(path),position_group_unit_evidence='exact reviewed columns')
    docs['new_member']=dict(content_sha256='e'*64,text='')
    if problem!='missing_review':
        a.sources['review']=dict(path=path.name,sha256=sha256_file(path),
            kind='visual_position_cell_review',url='')
    proof=dict(replacement)
    if problem=='different_scope':proof['effective_date']='2020-02-16'
    if problem=='changed_review':path.write_text('{"reviews": [], "changed": true}')
    def reader(archive,review,corporate,positions,*,source_urls=None):
        assert archive is a and review==path and source_urls=={'new_member'}
        return [proof]
    monkeypatch.setattr(builder,'position_source_review_candidates',reader)
    rows.append(replacement)
    if problem=='changed_review':
        with pytest.raises(ValueError,match='reviewed member source identity mismatch'):
            builder.repair_active_position_grade_boundaries(a,rows)
        return
    builder.repair_active_position_grade_boundaries(a,rows)
    old_future=any(r.get('extraction')=='source_composed_position_grade_boundary'
        and r['product']=='AA1' and r['effective_date']=='2020-03-01' for r in rows)
    assert old_future==(problem is not None)
    assert replacement in rows


def test_reviewed_member_ignores_a_different_nonformula_review_for_same_original(tmp_path, monkeypatch):
    a, rows, docs = active_grade_case(tmp_path)
    valid = tmp_path / 'group.json'
    valid.write_text('{"reviews": []}')
    other = tmp_path / 'numeric-cap.json'
    other.write_text('{"reviews": [], "unrelated": true}')
    replacement = dict(product='AA1', effective_date='2020-02-15', effective_phase='product_regular_open',
        known_at='2020-02-10T23:59:59+08:00', issue_date_bound=True,
        source_content_sha256='e'*64, source_url='new_member', unit='contracts',
        event_type='combined_position_formula', natural_person_limit=None,
        combined_position_base_product='AAF', combined_position_ratio='1/1',
        extraction_method='source_bound_visual_corporate_position_group',
        position_formula_parser='explicit_unchanged_unit_corporate_combination',
        visual_review_sha256=sha256_file(valid))
    docs['new_member'] = dict(content_sha256='e'*64, text='')
    for path in [valid, other]:
        a.sources[path.name] = dict(path=path.name, sha256=sha256_file(path),
            kind='visual_position_cell_review', url='')
    # The same announcement can own a separate numeric review. Its hash must
    # not request unrelated pages while validating this member's group proof.
    unrelated = dict(replacement, event_type='absolute_level', product='AAF', natural_person_limit=500,
                     visual_review_sha256=sha256_file(other), combined_position_ratio=None)
    rows.extend([replacement, unrelated])
    called = []
    def reader(archive, path, corporate, positions, *, source_urls=None):
        called.append(path)
        assert path == valid and source_urls == {'new_member'}
        return [replacement]
    monkeypatch.setattr(builder, 'position_source_review_candidates', reader)
    builder.repair_active_position_grade_boundaries(a, rows)
    assert called == [valid]
    assert not any(r.get('extraction') == 'source_composed_position_grade_boundary'
                   and r['product'] == 'AA1' and r['effective_date'] == '2020-03-01' for r in rows)
    assert unrelated in rows and replacement in rows


@pytest.mark.parametrize('problem', [None, 'missing_group', 'wrong_standard', 'wrong_units', 'late_clock'])
def test_retained_mixed_group_ends_only_source_proved_unchanged_member_generation(tmp_path, problem):
    a, rows, docs = active_grade_case(tmp_path)
    originals = deepcopy(rows)
    own = dict(product='AA1', from_product='AAF', effective_date='2020-02-15',
        contract_months=['202003', '202006'], contract_multiplier=2000., deliverable_security_quantity=2000.,
        source_url='mixed', announcement_url='mixed', source_content_sha256='e'*64,
        issue_date_bound=True, published_date='2020-02-10', known_at='2020-02-10T23:59:59+08:00')
    sibling = dict(own, product='AA2', from_product='AA1', contract_multiplier=2100.,
                   deliverable_security_quantity=2100.)
    financial = [own, sibling]
    native = ('調整生效日109年2月15日。AAF調整為AA1；AA1調整為AA2。'
              '二、加掛標準契約：上市日109年2月15日；契約代號AAF；'
              '約定標的物2,000股標的證券。三、部位限制：AAF、AA1與AA2部位合併計算。')
    docs['mixed'] = dict(content_sha256='e'*64, text=native)
    # This source alone proves the 2000-share member, while preserving the
    # different 2100-share member instead of fabricating another 1:1 ratio.
    parsed = builder.retained_corporate_position_text_candidates(a, financial, source_urls={'mixed'})
    assert len(parsed) == 1 and parsed[0]['product'] == 'AA1'
    replacement = parsed[0]
    rows.append(replacement)
    if problem == 'missing_group':
        docs['mixed']['text'] = native.replace('AAF、AA1與AA2部位合併計算。', '依另行公告辦理。')
    if problem == 'wrong_standard':
        docs['mixed']['text'] = native.replace('約定標的物2,000股', '約定標的物2,100股')
    if problem == 'wrong_units':
        financial = [dict(own, contract_multiplier=2200., deliverable_security_quantity=2200.), sibling]
    if problem == 'late_clock':
        replacement['known_at'] = '2020-02-15T12:00:00+08:00'
    builder.repair_active_position_grade_boundaries(a, rows, financial)
    old_future = [r for r in rows if r.get('extraction') == 'source_composed_position_grade_boundary'
                  and r['product'] == 'AA1' and r['effective_date'] == '2020-03-01']
    assert bool(old_future) == (problem is not None)
    assert rows[:len(originals)] == originals and replacement in rows
    assert not any(r.get('event_type') == 'combined_position_formula' and r['product'] == 'AA2' for r in rows)


@pytest.mark.parametrize('problem', [None, 'changed_named_units', 'missing_months', 'wrong_origin'])
def test_unchanged_pair_uses_only_its_explicit_members_monthly_units(problem):
    from stockagent.data.tw_futures_margin_preparation import unchanged_corporate_position_groups
    native = ('二、加掛標準契約：上市日104年10月14日；契約代號HWF；'
              '約定標的物2,000股標的證券。三、部位限制：HWF與HW1部位合併計算。')
    own = dict(product='HW1', from_product='HWF', effective_date='2015-10-14', issue_date_bound=True,
               contract_multiplier=2000., deliverable_security_quantity=2000., contract_months=['201511'])
    sibling = dict(own, product='HW2', from_product='HW1', contract_multiplier=2128.,
                   deliverable_security_quantity=2128., contract_months=['201510', '201512'])
    if problem == 'changed_named_units':
        own.update(contract_multiplier=2128., deliverable_security_quantity=2128.)
    if problem == 'missing_months':
        own['contract_months'] = []
    if problem == 'wrong_origin':
        own['from_product'] = 'ZZF'
    parsed = unchanged_corporate_position_groups(native, [own, sibling])
    if problem:
        assert parsed == []
    else:
        assert len(parsed) == 1 and parsed[0]['product'] == 'HW1'
        assert parsed[0]['combined_position_base_product'] == 'HWF'
        assert parsed[0]['combined_position_ratio'] == '1/1'
        assert parsed[0]['natural_person_limit'] is None


def printed_cap_successor_case(tmp_path, archive, docs):
    financial=dict(product='AA1',from_product='AAF',effective_date='2020-02-15',
        contract_months=['202006'],contract_multiplier=2200.,deliverable_security_quantity=2200.,
        source_url='printed_successor',announcement_url='printed_successor',source_content_sha256='e'*64,
        issue_date_bound=True,published_date='2020-02-10',known_at='2020-02-10T23:59:59+08:00')
    page=dict(page=0,native_text='AA1與AAF部位合併計算。',tables=[
        dict(cells=[['持有部位','AA1','AAF'],['每口折算股數','2,200','2,000']]),
        dict(caption='適用期間：自109年2月15日起至AA1契約終止掛牌前一營業日止',
             cells=[['自然人','法人機構','造市者'],['1,100,000股','3,300,000股','11,000,000股']])])
    table_path=tmp_path/'successor_tables.json'
    table_path.write_text(json.dumps(dict(pages=[page]),ensure_ascii=False))
    receipt_path=tmp_path/'successor_receipt.json'
    receipt_path.write_text(json.dumps(dict(status='complete',content_sha256='e'*64,
        files=[dict(path='tables.json',sha256=sha256_file(table_path))])))
    for path,kind in [(table_path,'native_table_tables.json'),(receipt_path,'native_table_receipt.json')]:
        archive.sources[path.name]=dict(url='printed_successor',path=path.name,kind=kind,sha256=sha256_file(path))
    docs['printed_successor']=dict(text=page['native_text'],content_sha256='e'*64)
    provenance={k:financial[k] for k in ('source_url','announcement_url','source_content_sha256',
        'issue_date_bound','published_date','known_at')}
    caps=[dict(r,**provenance) for r in builder.corporate_position_table_candidates([page],corporate=[financial])]
    assert {r['product'] for r in caps}=={'AA1','AAF'}
    return [financial],caps


def test_temporary_share_cap_uses_only_the_unique_printed_period_basis_and_keeps_both_caps(tmp_path):
    a,rows,docs=active_grade_case(tmp_path)
    for row in rows[3:]:row['natural_person_limit']=2100000.
    originals=deepcopy(rows)
    changes=builder.repair_active_position_grade_boundaries(a,rows)
    assert [r['corrected_share_limit'] for r in changes]==[1050000.,1575000.]
    assert all(r['grade_basis_position_units']==2100. and r['standard_position_units']==2000. for r in changes)
    assert rows[:5]==originals
    assert {r['position_unit'] for r in rows[5:]}=={2000.,2100.}
    intervals,issues=builder.position_candidate_intervals(rows)
    assert not issues
    for day,share_cap,contract_cap in [('2020-02-01',1050000.,500.),('2020-03-01',1575000.,750.)]:
        standard=next(r for r in intervals if r['product']=='AAF' and r['effective_date']==day)
        adjusted=next(r for r in intervals if r['product']=='AA1' and r['effective_date']==day)
        assert standard['position_limit']==adjusted['position_limit']==share_cap
        assert standard['position_unit']==2000. and adjusted['position_unit']==2100.
        assert standard['independent_contract_limit']==contract_cap
        assert adjusted['independent_contract_limit'] is None
    identical=deepcopy(rows)
    assert not builder.repair_active_position_grade_boundaries(a,rows) and rows==identical


@pytest.mark.parametrize('problem',['other_source','other_date','other_amount','unprinted_basis','no_clause'])
def test_temporary_cap_intersection_requires_the_exact_announced_grade(tmp_path,problem):
    a,rows,docs=active_grade_case(tmp_path)
    for row in rows[3:]:row['natural_person_limit']=2100000.
    builder.repair_active_position_grade_boundaries(a,rows)
    if problem=='other_source':rows[1]['source_url']='other_quarter'
    if problem=='other_date':rows[1]['effective_date']='2020-02-02'
    if problem=='other_amount':rows[1]['natural_person_limit']=400.
    if problem in ('unprinted_basis','no_clause'):
        for row in rows[5:]:
            if row['effective_date']!='2020-02-01':continue
            proof=json.loads(row['position_grade_evidence'])
            if problem=='unprinted_basis':proof['grade_basis_position_units']=2000.
            else:proof['clause_scope']='unproved'
            row['position_grade_evidence']=json.dumps(proof,sort_keys=True)
    intervals,issues=builder.position_candidate_intervals(rows)
    assert all(r['independent_contract_limit'] is None for r in intervals
               if r['product']=='AAF' and r['effective_date']=='2020-02-01')
    if problem=='other_date':
        later=next(r for r in intervals if r['product']=='AAF' and r['effective_date']=='2020-02-02')
        assert later['event_type']=='absolute_level' and later['position_limit']==500.
    else:assert issues


@pytest.mark.parametrize('already_composed',[False,True])
def test_new_printed_share_cap_bounds_old_grades_and_their_automatic_descendants(tmp_path,already_composed):
    a,rows,docs=active_grade_case(tmp_path)
    original_printed=deepcopy(rows)
    if already_composed:
        builder.repair_active_position_grade_boundaries(a,rows)
        third=dict(rows[2],effective_date='2020-03-15',natural_person_limit=1000,
            known_at='2020-03-10T23:59:59+08:00',source_url='quarter3',source_content_sha256='f'*64)
        docs['quarter3']=dict(content_sha256='f'*64,text=docs['quarter2']['text'])
        rows.append(third)
        builder.repair_active_position_grade_boundaries(a,rows)
        assert any(json.loads(r.get('position_grade_evidence') or '{}').get('origin_effective_date')
            =='2020-03-01' for r in rows)
    financial,caps=printed_cap_successor_case(tmp_path,a,docs)
    rows.extend(caps)
    changes=builder.repair_active_position_grade_boundaries(a,rows,financial)
    assert rows[:5]==original_printed and all(cap in rows for cap in caps)
    derived=[r for r in rows if r.get('extraction')=='source_composed_position_grade_boundary'
             and r['source_content_sha256']=='c'*64]
    assert {(r['product'],r['effective_date']) for r in derived}=={
        ('AA1','2020-02-01'),('AAF','2020-02-01')}
    assert all(r['valid_until_date_inclusive']=='2020-02-14' for r in derived)
    assert all(json.loads(r['position_grade_evidence'])['valid_until_date_exclusive']
        =='2020-02-15' for r in derived)
    new_derived=[r for r in rows if r.get('extraction')=='source_composed_position_grade_boundary'
        and r['source_content_sha256']=='e'*64]
    assert {(r['product'],r['effective_date']) for r in new_derived}=={
        (product,day) for product in ['AA1','AAF']
        for day in (['2020-03-01','2020-03-15'] if already_composed else ['2020-03-01'])}
    assert all(json.loads(r['position_grade_evidence'])['origin_effective_date']=='2020-02-15'
        for r in new_derived)
    retired=json.loads((tmp_path/'superseded_composed_position_grades.json').read_text())
    assert len(retired)==(6 if already_composed else 0)
    assert all(r['reason']=='old_origin_derived_grade_crosses_printed_cap' for r in retired)
    assert all(r['successor']['source_content_sha256']=='e'*64 for r in retired)
    assert all(r['printed_origin_effective_date']=='2020-01-10' for r in retired)
    states,issues=builder.position_candidate_intervals(rows)
    assert not issues
    assert {(r['product'],r['position_unit'],r['position_limit']) for r in states
        if r['effective_date']=='2020-02-15'}=={('AA1',2200.,1100000.),('AAF',2000.,1100000.)}
    identical=deepcopy(rows)
    assert not builder.repair_active_position_grade_boundaries(a,rows,financial) and rows==identical
    assert len(changes)==(3 if already_composed else 2)


@pytest.mark.parametrize('problem',['missing_months','missing_member','conflicting_cap','late',
                                  'unknown_phase','wrong_unit','wrong_source'])
def test_printed_successor_requires_own_literal_units_complete_group_and_known_clock(tmp_path,problem):
    a,rows,docs=active_grade_case(tmp_path)
    builder.repair_active_position_grade_boundaries(a,rows)
    original=deepcopy(rows)
    financial,caps=printed_cap_successor_case(tmp_path,a,docs)
    if problem=='missing_months':financial[0]['contract_months']=[]
    if problem=='missing_member':caps.pop()
    if problem=='conflicting_cap':caps.append(dict(caps[0],natural_person_limit=1200000.))
    if problem=='late':
        for r in caps:r['known_at']='2020-02-15T23:59:59+08:00'
    if problem=='unknown_phase':
        for r in caps:r['effective_phase']='date_only_requires_phase_review'
    if problem=='wrong_unit':financial[0]['contract_multiplier']=2300.
    if problem=='wrong_source':docs['printed_successor']['content_sha256']='f'*64
    rows.extend(caps)
    if problem=='wrong_source':
        with pytest.raises(ValueError,match='source identity drift'):
            builder.repair_active_position_grade_boundaries(a,rows,financial)
    else:
        builder.repair_active_position_grade_boundaries(a,rows,financial)
        assert not json.loads((tmp_path/'superseded_composed_position_grades.json').read_text())
    assert all(r in rows for r in original)


def original_position_restoration_case(tmp_path):
    old=dict(product='IQF',effective_date='2011-05-03',effective_phase='new_contract_listing',
        natural_person_limit=350.,unit='contracts',event_type='absolute_level',
        source_url='listing',source_content_sha256='a'*64,issue_date_bound=True,
        published_date='2011-04-26',known_at='2011-04-26T23:59:59+08:00')
    corporate=dict(from_product='IQF',product='IQ1',effective_date='2011-08-12',
        contract_months=['201109','201112','201203','201206'],contract_multiplier=2000.,
        deliverable_security_quantity=2000.,
        deliverable_components_resolved=True,deliverable_has_subscription_rights=True,
        source_url='adjustment',source_content_sha256='b'*64,issue_date_bound=True,
        published_date='2011-08-08',known_at='2011-08-08T23:59:59+08:00')
    text=('自100年9月22日起，IQF與IQ1部位合併計算，'
          '並恢復為原公告之交易人部位限制數控管。')
    docs={'listing':dict(content_sha256='a'*64,text='原上市公告自然人350口'),
          'adjustment':dict(content_sha256='b'*64,text=text)}
    a=SimpleNamespace(bundle=tmp_path,sources={},document=lambda url:docs[url])
    return a,[old],[corporate],docs


def test_original_position_restoration_joins_prior_cap_and_unchanged_units(tmp_path):
    a,rows,corporate,docs=original_position_restoration_case(tmp_path)
    repairs=builder.repair_original_position_restorations(a,rows,corporate)
    assert len(repairs)==1 and repairs[0]['restored_contract_limit']==350.
    assert repairs[0]['effective_date']=='2011-09-22'
    intervals,issues=builder.position_candidate_intervals(rows)
    assert not issues
    restored=next(r for r in intervals if r['product']=='IQF'
                  and r['effective_date']=='2011-09-22')
    assert restored['position_limit']==350. and restored['unit']=='contracts'
    assert restored['known_at']=='2011-08-08T23:59:59+08:00'
    assert restored['source_content_sha256s']==['a'*64,'b'*64]
    adjusted=next(r for r in intervals if r['product']=='IQ1')
    assert (adjusted['conversion_numerator'],adjusted['conversion_denominator'])==(1,1)
    # Rebuilds replace their own composition instead of multiplying facts.
    expected=deepcopy(rows)
    assert builder.repair_original_position_restorations(a,rows,corporate)==repairs
    assert rows==expected
    docs['listing']['content_sha256']='c'*64
    with pytest.raises(ValueError,match='prior source mismatch'):
        builder.repair_original_position_restorations(a,rows,corporate)


def named_position_restoration_case(tmp_path):
    from stockagent.data.tw_futures_margin_preparation import corporate_position_table_candidates
    clause=('CJ2契約終止掛牌前，部位限制依下列股數標準辦理；'
            'CJ2契約終止掛牌後，恢復以部位限制契約數控管。')
    page=dict(page=4,native_text=('CJ1、CJ2與CJF部位合併計算；'
        'CJA與CJO同方向選擇權部位合併計算。'+clause),tables=[dict(cells=[
            ['持有部位','CJF','CJ1','CJ2','CJO','CJA'],
            ['每口折算股數','2,000','2,000','2,120','2,000','2,000'],
            ['自然人','2,500,000股'],['法人機構','7,500,000股'],['造市者','18,700,000股']])])
    provenance=dict(source_url='adjustment',source_content_sha256='b'*64,
        issue_date_bound=True,published_date='2011-10-31',known_at='2011-10-31T23:59:59+08:00')
    corporate=[dict(provenance,from_product=old,product=code,effective_date='2011-11-07',
        contract_months=months,contract_multiplier=units,deliverable_security_quantity=units,
        deliverable_components_resolved=True,deliverable_has_subscription_rights=True)
        for code,old,units,months in [('CJ1','CJF',2000.,['201111','201112','201203','201206','201209']),
                                    ('CJ2','CJ1',2120.,['201112'])]]
    caps=[dict(r,**provenance) for r in corporate_position_table_candidates([page],corporate=corporate)]
    old=dict(product='CJF',effective_date='2010-01-25',effective_phase='new_contract_listing',
        natural_person_limit=1250.,unit='contracts',event_type='absolute_level',
        source_url='listing',source_content_sha256='a'*64,issue_date_bound=True,
        published_date='2010-01-15',known_at='2010-01-15T23:59:59+08:00')
    docs={'listing':dict(content_sha256='a'*64,text='原上市公告自然人1250口'),
          'adjustment':dict(content_sha256='b'*64,text=page['native_text'])}
    archive=SimpleNamespace(bundle=tmp_path,sources={},document=lambda url:docs[url])
    days=[date(2011,11,7),date(2011,11,8),date(2011,12,21),date(2011,12,22)]
    life=pl.DataFrame([dict(product='CJ2',contract='201112',
        first_observed_date=days[0],last_observed_date=days[2],official_expiry=days[2],
        calendar_end=days[2],first_zero_oi_date=None,final_settlement_price=16.86,
        lifetime_status='official_final',final_fixing_origin='official_product_final',
        settlement_method='cash_settlement')])
    raw=pl.DataFrame(dict(product=['CJ2']*3,contract=['201112']*3,date=days[:3],open_interest=[9.,6.,6.]))
    proof={key:'c'*64 for key in ['physical_lifetimes','original_daily_source',
        'official_final_source','calendar_source','dated_group_law']}
    law=dict(asset_class='stock_future',standard_units=2000,effective_date='2010-01-25',
        known_at='2010-01-08T23:59:59+08:00',source_content_sha256='d'*64,
        rule='same_security_same_direction; unchanged_units_or_explicit_securities_cap_only')
    kwargs=dict(lifetimes=life,observations=raw,market_dates=days,
        termination_proof_sha256s=proof,termination_group_rules=[law])
    return archive,[old,*caps],corporate,kwargs,page,docs


def test_named_termination_parser_preserves_literal_cap_and_defers_its_end(tmp_path):
    _,rows,_,_,_,_=named_position_restoration_case(tmp_path)
    caps=rows[1:]
    assert {r['product']:r['position_unit'] for r in caps}=={'CJF':2000.,'CJ1':2000.,'CJ2':2120.}
    assert all(r['natural_person_limit']==2500000. and r['requires_delisting_clock'] for r in caps)
    assert all(r['termination_product']=='CJ2' and r['valid_until_date_inclusive'] is None for r in caps)
    intervals,issues=builder.position_candidate_intervals(rows)
    assert len(intervals)==1 and intervals[0]['product']=='CJF'
    assert {r['product'] for r in issues if r['reasons']=='named_position_termination_clock_unresolved'}=={'CJF','CJ1','CJ2'}


@pytest.mark.parametrize('change',['missing_option_group','missing_clause','wrong_unit','zero_unit',
                                   'wrong_member','missing_person','ambiguous_group'])
def test_named_termination_parser_does_not_guess_ragged_grid_ownership(tmp_path,change):
    from stockagent.data.tw_futures_margin_preparation import corporate_position_table_candidates
    _,_,corporate,_,page,_=named_position_restoration_case(tmp_path)
    cells=page['tables'][0]['cells']
    if change=='missing_option_group':page['native_text']=page['native_text'].replace('CJA與CJO同方向選擇權部位合併計算。','')
    elif change=='missing_clause':page['native_text']=page['native_text'].replace('恢复','').replace('恢復','不確定')
    elif change=='wrong_unit':cells[1][2]='2,001'
    elif change=='zero_unit':cells[1][2]='0'
    elif change=='wrong_member':cells[0][-1]='XYZ'
    elif change=='missing_person':cells[3][0]='其他'
    else:page['native_text']+='IA1與IAF部位合併計算。'
    assert not corporate_position_table_candidates([page],corporate=corporate)


def test_named_termination_composes_dated_cap_only_after_complete_official_end(tmp_path):
    a,rows,corporate,kwargs,_,_=named_position_restoration_case(tmp_path)
    repairs=builder.repair_original_position_restorations(a,rows,corporate,**kwargs)
    assert len(repairs)==1 and repairs[0]['effective_date']=='2011-12-22'
    assert repairs[0]['conditional_terminal_date']=='2011-12-21'
    assert repairs[0]['restored_contract_limit']==1250.
    assert repairs[0]['combined_products']==['CJ1','CJF']
    assert repairs[0]['known_at']=='2011-12-21T23:59:59+08:00'
    assert all(r['valid_until_date_inclusive']=='2011-12-21' and not r['requires_delisting_clock']
               for r in rows if r.get('termination_product')=='CJ2')
    intervals,issues=builder.position_candidate_intervals(rows)
    assert not issues
    restored=[r for r in intervals if r['effective_date']=='2011-12-22']
    assert {r['product'] for r in restored}=={'CJF','CJ1'}
    assert all(r['known_at']=='2011-12-21T23:59:59+08:00' for r in restored)
    expected=deepcopy(rows)
    assert builder.repair_original_position_restorations(a,rows,corporate,**kwargs)==repairs
    assert rows==expected
    # Losing the bound terminal proof on reuse revokes the derived clock.
    assert not builder.repair_original_position_restorations(a,rows,corporate)
    assert all(r['requires_delisting_clock'] and r['valid_until_date_inclusive'] is None
               for r in rows if r.get('termination_product')=='CJ2')
    assert not any(r.get('extraction')=='source_composed_original_position_restoration' for r in rows)


@pytest.mark.parametrize('change',['missing_observation','duplicate_observation','null_oi','early_zero_oi',
    'wrong_month','quote_only_end','early_last_quote','no_following_session','missing_final_price',
    'wrong_units','later_known_base','different_base_capacity','missing_law','future_law','ambiguous_law',
    'missing_clause'])
def test_named_termination_keeps_incomplete_source_chains_blocked(tmp_path,change):
    a,rows,corporate,kwargs,_,docs=named_position_restoration_case(tmp_path)
    if change=='missing_observation':kwargs['observations']=kwargs['observations'].slice(1)
    elif change=='duplicate_observation':kwargs['observations']=pl.concat([kwargs['observations'],kwargs['observations'].head(1)])
    elif change=='null_oi':kwargs['observations']=kwargs['observations'].with_columns(pl.lit(None).cast(pl.Float64).alias('open_interest'))
    elif change=='early_zero_oi':kwargs['observations']=kwargs['observations'].with_columns(pl.lit(0.).alias('open_interest'))
    elif change=='wrong_month':corporate[1]['contract_months']=['201203']
    elif change=='quote_only_end':kwargs['lifetimes']=kwargs['lifetimes'].with_columns(pl.lit('last_quote').alias('final_fixing_origin'))
    elif change=='early_last_quote':kwargs['lifetimes']=kwargs['lifetimes'].with_columns(pl.lit(date(2011,12,20)).alias('last_observed_date'))
    elif change=='no_following_session':kwargs['market_dates']=kwargs['market_dates'][:-1]
    elif change=='missing_final_price':kwargs['lifetimes']=kwargs['lifetimes'].with_columns(pl.lit(None).cast(pl.Float64).alias('final_settlement_price'))
    elif change=='wrong_units':corporate[0]['contract_multiplier']=2001.
    elif change=='later_known_base':rows[0]['known_at']='2011-11-01T23:59:59+08:00'
    elif change=='different_base_capacity':rows[0]['natural_person_limit']=350.
    elif change=='missing_law':kwargs['termination_group_rules']=None
    elif change=='future_law':kwargs['termination_group_rules'][0]['effective_date']='2012-01-01'
    elif change=='ambiguous_law':kwargs['termination_group_rules']*=2
    else:docs['adjustment']['text']=docs['adjustment']['text'].replace('恢復','不確定')
    if change=='missing_clause':
        with pytest.raises(ValueError,match='retained literal clause'):
            builder.repair_original_position_restorations(a,rows,corporate,**kwargs)
    else:
        assert not builder.repair_original_position_restorations(a,rows,corporate,**kwargs)
    assert not any(r.get('extraction')=='source_composed_original_position_restoration' for r in rows)
    assert all(r['requires_delisting_clock'] for r in rows if r.get('termination_product')=='CJ2')


@pytest.mark.parametrize('change',['valid','source_drift','lifetime_dependency','law_original','unsafe_law_path','out_of_scope'])
def test_named_termination_context_verifies_source_ownership_before_composition(tmp_path,monkeypatch,change):
    import gzip,hashlib
    _,_,_,kwargs,_,_=named_position_restoration_case(tmp_path)
    names=['physical_lifetimes','original_daily_source','official_final_source','calendar_source']
    paths={name:tmp_path/(name+'.parquet') for name in names}
    for name,path in paths.items():path.write_bytes(name.encode())
    raw=tmp_path/'law.body.gz';content=b'dated original stock futures rule';raw.write_bytes(gzip.compress(content))
    original=hashlib.sha256(content).hexdigest()
    rule=dict(kwargs['termination_group_rules'][0],source_content_sha256=original,
              effectiveness_source_content_sha256=original)
    law=dict(review_kind='source_bound_position_family_rules',rules=[rule],
        sources=[dict(path=raw.name,sha256=sha256_file(raw),kind='raw_gzip')])
    if change=='law_original':law['rules'][0]['source_content_sha256']='e'*64
    if change=='unsafe_law_path':law['sources'][0]['path']='../law.body.gz'
    paths['dated_group_law']=tmp_path/'law.json';paths['dated_group_law'].write_text(json.dumps(law))
    context=tmp_path/'context.json';context.write_text(json.dumps(dict(selected_products=['CJ2'],
        sources={k:dict(path=str(v),sha256=sha256_file(v)) for k,v in paths.items()})))
    dependencies={str(paths[k]):sha256_file(paths[k]) for k in names[1:]}
    if change=='lifetime_dependency':dependencies[str(paths['official_final_source'])]='f'*64
    def read(path,**_):
        if path==paths['physical_lifetimes']:return kwargs['lifetimes'],dict(source_sha256s=dependencies)
        if path==paths['original_daily_source']:return kwargs['observations'],{}
        assert path==paths['calendar_source']
        return pl.DataFrame({'date':kwargs['market_dates']}),{}
    monkeypatch.setattr('stockagent.data.tw_futures_margin_release.read_bound_output',read)
    if change=='source_drift':paths['original_daily_source'].write_bytes(b'changed original')
    terminal=['IA1'] if change=='out_of_scope' else ['CJ2']
    if change!='valid':
        with pytest.raises(ValueError,match='SHA mismatch|dependency mismatch|retained original|out-of-scope'):
            builder.load_named_position_termination_context(context,termination_products=terminal)
    else:
        loaded,sources=builder.load_named_position_termination_context(context,termination_products=terminal)
        assert loaded['lifetimes'].equals(kwargs['lifetimes'])
        assert loaded['observations'].equals(kwargs['observations'])
        assert sorted(loaded['market_dates'])==kwargs['market_dates']
        assert sources[str(context)]==sha256_file(context) and sources[str(raw)]==sha256_file(raw)


def test_margin_only_extension_retains_dated_position_compositions(tmp_path):
    a, rows, corporate, _ = original_position_restoration_case(tmp_path)
    builder.repair_original_position_restorations(a, rows, corporate)
    reviewed = dict(product='IQF', source_content_sha256='b'*64,
        effective_date='2011-08-12', natural_person_limit=700000., unit='shares',
        extraction_method='source_bound_visual_corporate_position_cells')
    flawed = dict(reviewed, natural_person_limit=70000., extraction_method='ocr')
    grade = dict(reviewed, effective_date='2011-09-15', natural_person_limit=1000000.,
        extraction='source_composed_position_grade_boundary', extraction_method='native')
    sibling = dict(flawed, product='IAF')
    prior = [*rows, reviewed, flawed, grade, sibling]
    result = builder.prioritize_reviewed_position_facts(prior)
    assert flawed not in result
    assert all(row in result for row in [*rows, reviewed, grade, sibling])
    assert builder.prioritize_reviewed_position_facts(result) == result
    restored = [r for r in result if r.get('extraction') ==
                'source_composed_original_position_restoration']
    assert {r['product'] for r in restored} == {'IQF', 'IQ1'}
    # A new review explicitly supersedes the old source/product first; the
    # priority pass cannot restore a composition removed by that review.
    replacement = dict(reviewed, natural_person_limit=710000.)
    replaced = builder.replace_reviewed_product_facts(result, [replacement])
    final = builder.prioritize_reviewed_position_facts(replaced)
    assert replacement in final and grade not in final
    assert not any(r['product'] == 'IQF' for r in final if r in restored)


def test_parent_position_context_facts_survive_unrelated_extension():
    reviewed = dict(product='IQF', source_content_sha256='b'*64,
        extraction_method='source_bound_visual_corporate_position_cells',
        effective_date='2011-08-12', natural_person_limit=700000.)
    sibling = dict(reviewed, extraction_method='explicit_source_context',
        effective_date='2011-09-22', natural_person_limit=350.)
    inherited = [reviewed, sibling]
    fresh_ocr = dict(reviewed, extraction_method='ocr', natural_person_limit=70000.)
    assert builder.prioritize_reviewed_position_facts([*inherited, fresh_ocr], inherited) == inherited
    replacement = dict(reviewed, natural_person_limit=710000.)
    current = builder.replace_reviewed_product_facts(inherited, [replacement])
    assert builder.prioritize_reviewed_position_facts(current, inherited) == [replacement]


@pytest.mark.parametrize('change',['changed_units','later_known_cap','intervening_grade',
                                  'missing_cap','ambiguous_day','unrelated_member','missing_clause'])
def test_original_position_restoration_keeps_unsupported_references_missing(tmp_path,change):
    a,rows,corporate,docs=original_position_restoration_case(tmp_path)
    if change=='changed_units':corporate[0]['contract_multiplier']=2039.9273
    elif change=='later_known_cap':rows[0]['known_at']='2011-08-09T23:59:59+08:00'
    elif change=='intervening_grade':rows.append(dict(rows[0],effective_date='2011-08-20',
        natural_person_limit=1250.,source_content_sha256='c'*64))
    elif change=='missing_cap':rows.clear()
    elif change=='ambiguous_day':docs['adjustment']['text']+='自100年9月23日起，IQF與IQ1部位合併計算，並恢復為原公告之交易人部位限制數控管。'
    elif change=='unrelated_member':docs['adjustment']['text']=docs['adjustment']['text'].replace('IQ1','IA1')
    else:docs['adjustment']['text']=docs['adjustment']['text'].replace('原公告','目前公告')
    expected=deepcopy(rows)
    assert not builder.repair_original_position_restorations(a,rows,corporate)
    assert rows==expected


def multi_product_restoration_notice():
    return ('發文日期：中華民國115年1月7日。本次調整自115年1月8日一般交易時段結束後起實施，'
        '證券市場處置期間結束後，群創期貨及群創選擇權、群聯期貨及小型群聯期貨'
        '於115年1月20日一般交易時段結束後恢復為115年1月8日調整前之保證金，'
        '威剛期貨於115年1月22日一般交易時段結束後恢復為115年1月5日調整前之保證金。'
        '本公司115年1月2日台期結字第11503000081號函自115年1月8日威剛期貨'
        '一般交易時段結束後停止適用。單位：比例(%)DQF(群創期貨)NDF(威剛期貨)'
        'NWF(群聯期貨)QNF(小型群聯期貨)')


def test_multiple_restoration_subjects_keep_their_dates_and_before_references():
    text=multi_product_restoration_notice()
    clock=builder.candidate_notice_clock(text,'2026-01-07')
    assert clock['effective_date']=='2026-01-08'
    for product,day in [('DQF','2026-01-20'),('NWF','2026-01-20'),
                        ('QNF','2026-01-20'),('NDF','2026-01-22')]:
        scoped=builder.product_margin_restoration_clock(text,product,clock)
        assert scoped['restoration_clock_scope']=='explicit_product_clause'
        assert {e['date_iso'] for e in json.loads(scoped['temporary_end_evidence'])}=={day}
    fact=dict(product='DQF',effective_date='2026-01-08',before=[.135,.1035,.1],
        margin_kind='notional_rate',known_at='2026-01-07T23:59:59+08:00',issue_date_bound=True)
    target=builder.referenced_before_restoration([text],fact,[])
    assert target==dict(restoration_rule='return_to_declared_before',restoration_target=fact['before'])
    prior=dict(fact,product='NDF',effective_date='2026-01-05',before=[.2025,.1553,.15],
        source_content_sha256='a'*64,known_at='2026-01-02T23:59:59+08:00')
    ndf=dict(fact,product='NDF',before=[.3038,.2329,.225])
    target=builder.referenced_before_restoration([text],ndf,[prior])
    assert target['restoration_rule']=='return_to_referenced_before'
    assert target['restoration_target']==prior['before']
    assert not builder.referenced_before_restoration([text],ndf,[])


def test_mini_name_and_conflicting_explicit_code_do_not_assign_standard_future():
    text=('小型群聯期貨於115年1月20日一般交易時段結束後恢復為調整前之保證金。'
          'NWF(群聯期貨)QNF(小型群聯期貨)')
    assigned=builder.named_margin_restoration_clauses(text)
    assert assigned[0]['products']=={'QNF'}
    broken=text.replace('小型群聯期貨於','小型群聯期貨契約(NWF)於')
    assert builder.named_margin_restoration_clauses(broken) is None
    # One parsed group cannot hide another unparsed recovery predicate.
    partial=text+'另有契約於115年1月21日一般交易時段結束後恢復為調整前之保證金。'
    assert builder.named_margin_restoration_clauses(partial) is None


def test_repair_of_old_notice_revocation_uses_product_scoped_new_end(tmp_path):
    text=multi_product_restoration_notice()
    a=SimpleNamespace(bundle=tmp_path,sources={},document=lambda url:dict(text=text,content_sha256='b'*64))
    old=builder.candidate_notice_clock(text,'2026-01-07')
    row=dict(old,product='NDF',source_url='notice',source_content_sha256='b'*64,
        published_date='2026-01-07',known_at='2026-01-07T23:59:59+08:00',
        after=[.405,.3105,.3],before=[.3038,.2329,.225],margin_kind='notional_rate')
    # A legacy parent had mixed product ends and the old notice's revocation.
    ends=json.loads(row['temporary_end_evidence'])
    ends.append(dict(date_iso='2026-01-08',boundary='after_regular_session',role='effective_end',
        evidence='台期結字第11503000081號函自115年1月8日威剛期貨一般交易時段結束後停止適用'))
    row['temporary_end_evidence']=json.dumps(ends,ensure_ascii=False)
    assert builder.repair_missing_margin_source_context(a,[row])
    assert {e['date_iso'] for e in json.loads(row['temporary_end_evidence'])}=={'2026-01-22'}


def test_restoration_does_not_borrow_immediately_prior_amount_for_earlier_reference():
    assert builder.has_local_before_restoration(['恢復為調整前之保證金'], '2026-01-12')
    assert builder.has_local_before_restoration(['恢復為115年1月12日調整前保證金'], '2026-01-12')
    for clause in ['恢復為115年1月7日調整前保證金', '恢復為1月7日調整前之保證金']:
        assert not builder.has_local_before_restoration([clause], '2026-01-12')
        assert not builder.has_local_before_restoration(['恢復為調整前之保證金', clause], '2026-01-12')


def test_closure_postponement_requires_the_literal_original_clause():
    clause='調整期間如遇休市、有價證券停止買賣、全\n日暫停交易，則恢復日順延執行。'
    assert builder.has_closed_cash_postponement_clause([clause])
    assert builder.has_closed_cash_postponement_clause([builder.compact(clause)])
    assert not builder.has_closed_cash_postponement_clause(['如遇休市則恢復日不順延執行'])
    assert not builder.has_closed_cash_postponement_clause([clause.replace('則恢復日順延執行','另行公告')])


def test_restoration_short_date_needs_unique_explicit_year_and_forward_boundary():
    text=('臺灣期貨交易所新聞稿中華民國109年3月23日 '
          '自109年3月24日該契約交易時段結束後起實施，'
          '4月7日該契約交易時段結束後恢復為調整前之保證金。')
    row=builder.candidate_notice_clock(text,'2020-03-23')
    assert any(e['date_iso']=='2020-04-07' and e['boundary']=='after_trading_session'
               for e in json.loads(row['temporary_end_evidence']))
    for ambiguous in [text+'參照108年1月1日公告。',text.replace('4月7日','3月7日')]:
        row=builder.candidate_notice_clock(ambiguous,'2020-03-23')
        assert not any(e.get('year_binding') for e in json.loads(row['temporary_end_evidence']))


@pytest.mark.parametrize('first_date', ['9月8日', '115年9月8日'])
def test_named_products_with_different_dates_share_a_complete_regular_close_predicate(first_date):
    text=('臺灣期貨交易所新聞稿中華民國115年9月2日。'
          '本次保證金調整商品為玉晶光期貨(LEF)、小型玉晶光期貨(QJF)、'
          '金居期貨(PQF)及精材期貨(QLF)，'
          '自115年9月3日一般交易時段結束後起實施。'
          '配合證券市場處置期間結束，玉晶光期貨及小型玉晶光期貨於'+first_date+'、'
          '金居期貨及精材期貨於9月10日一般交易時段結束後，恢復為調整前之保證金。')
    base=builder.candidate_notice_clock(text,'2026-09-02')
    expected={'LEF':'2026-09-08','QJF':'2026-09-08','PQF':'2026-09-10','QLF':'2026-09-10'}
    for product,day in expected.items():
        scoped=builder.product_margin_restoration_clock(text,product,base)
        ends=json.loads(scoped['temporary_end_evidence'])
        assert len(ends)==1 and ends[0]['date_iso']==day
        assert ends[0]['boundary']=='after_regular_session'
        assert ends[0]['product_scope']==product
        assert scoped['effective_date']==base['effective_date']=='2026-09-03'
        assert scoped['restoration_clock_scope']=='explicit_product_clause'
    assert builder.product_margin_restoration_clock(text,'TX',base)==base


@pytest.mark.parametrize('problem', [
    'unbound_publication','mixed_years','unknown_first','unknown_last',
    'missing_close','conflicting_product_dates','before_start','missing_year',
])
def test_shared_short_restoration_predicate_does_not_admit_partial_or_ambiguous_ownership(problem):
    text=('臺灣期貨交易所新聞稿中華民國115年9月2日。'
          '本次商品為甲期貨(AAF)及乙期貨(BBF)。'
          '自115年9月3日一般交易時段結束後起實施。'
          '甲期貨於9月8日、乙期貨於9月10日一般交易時段結束後，恢復為調整前之保證金。')
    if problem=='mixed_years':text+='另參照114年1月1日公告。'
    if problem=='unknown_first':text=text.replace('甲期貨於','未命名商品於9月7日、甲期貨於')
    if problem=='unknown_last':text=text.replace('乙期貨於','未命名商品於')
    if problem=='missing_close':text=text.replace('一般交易時段結束後，恢復','一般交易時段，恢復')
    if problem=='conflicting_product_dates':text=text.replace('乙期貨於','甲期貨於')
    if problem=='before_start':text=text.replace('9月8日','9月1日')
    if problem=='missing_year':text=text.replace('115年','')
    base=dict(effective_date='2026-09-03',effective_phase='after_product_regular_close',
              issue_date_bound=problem!='unbound_publication',
              temporary_end_evidence='[]',requires_reversion_review=True)
    assert builder.product_margin_restoration_clock(text,'AAF',base)==base
    assert builder.product_margin_restoration_clock(text,'BBF',base)==base


def test_short_restoration_year_excludes_only_a_numbered_quoted_legal_reference():
    text=('臺灣期貨交易所新聞稿中華民國105年1月14日。'
          '臺灣期貨交易所依104年7月31日台期監字10410007970號函'
          '「股票期貨及股票選擇權契約保證金之處置調整措施」，'
          '本次保證金調整自105年1月15日該股票期貨契約交易時段結束後起實施，'
          '並於1月27日該契約交易時段結束後恢復為1月15日調整前之保證金。')
    row=builder.candidate_notice_clock(text,'2016-01-14')
    ends=json.loads(row['temporary_end_evidence'])
    assert len(ends)==1 and ends[0]['date_iso']=='2016-01-27'
    assert ends[0]['boundary']=='after_regular_session'
    assert ends[0]['year_binding']=='unique_explicit_roc_year_excluding_numbered_legal_reference'
    for ambiguous in [text.replace('10410007970號函','號函'),
                      text.replace('「股票期貨及股票選擇權契約保證金之處置調整措施」','公告'),
                      text+'另參照104年1月1日公告。',
                      text.replace('1月27日','1月7日'),
                      text.replace('105年1月15日','106年1月15日')]:
        result=builder.candidate_notice_clock(ambiguous,'2016-01-14')
        assert not any(e.get('year_binding') for e in json.loads(result['temporary_end_evidence']))


def test_enumerated_product_restoration_dates_share_only_the_printed_regular_close():
    text=('自114年8月21日一般交易時段結束後起實施，並於證券市場處置期間結束後，'
          '台玻期貨(KUF)於114年9月4日，長興期貨(QOF)於114年9月2日，'
          '該契約一般交易時段結束後恢復為114年8月21日調整前之保證金。')
    base=dict(effective_date='2025-08-21',effective_phase='after_product_regular_close',
              temporary_end_evidence='[]',requires_reversion_review=True)
    for product,day in [('KUF','2025-09-04'),('QOF','2025-09-02')]:
        row=builder.product_margin_restoration_clock(text,product,base)
        ends=json.loads(row['temporary_end_evidence'])
        assert len(ends)==1 and ends[0]['date_iso']==day
        assert ends[0]['boundary']=='after_regular_session' and ends[0]['product_scope']==product
        assert row['effective_date']==base['effective_date']
    assert builder.product_margin_restoration_clock(text,'TX',base)==base
    for ambiguous in [text.replace('QOF','KUF'),
                      text.replace('一般交易時段結束後恢復為','一般交易時段恢復為'),
                      text.replace('114年9月2日','114年8月20日'),
                      text+'另114年9月5日一般交易時段結束後恢復原保證金。']:
        assert builder.product_margin_restoration_clock(ambiguous,'QOF',base)==base


def test_plain_prior_notice_rule_revocation_keeps_the_new_product_restoration(tmp_path):
    text=('發文日期：中華民國115年6月2日。發文字號：台期結字第11503011611號。'
          '本次調整自115年6月3日一般交易時段結束後起實施。'
          '115年6月15日一般交易時段結束後，力積電期貨契約(QZF)恢復為115年6月3日調整前保證金。'
          '本公司115年5月28日台期結字第11503011071號函有關合晶期貨保證金之規定，'
          '自115年6月3日一般交易時段結束後停止適用。')
    clock=builder.candidate_notice_clock(text,'2026-06-02')
    ends=json.loads(clock['temporary_end_evidence'])
    assert not any(e['date_iso']=='2026-06-03' for e in ends)
    scoped=builder.product_margin_restoration_clock(text,'QZF',clock)
    assert {e['date_iso'] for e in json.loads(scoped['temporary_end_evidence'])}=={'2026-06-15'}
    proof=json.loads(clock['prior_notice_revocation_evidence'])
    assert len(proof)==1 and proof[0]['revoked_notice_number']=='11503011071'
    original=dict(product='QZF',source_url='source',source_content_sha256='a'*64,
        published_date='2026-06-02',known_at='2026-06-02T23:59:59+08:00',
        issue_date_bound=True,effective_date='2026-06-03',
        effective_phase='after_product_regular_close',temporary_end_evidence=json.dumps([proof[0]]),
        requires_reversion_review=True,margin_kind='notional_rate',
        after=[.3038,.2329,.225],before=[.2025,.1553,.15])
    archive=SimpleNamespace(bundle=tmp_path,sources={},document=lambda url:dict(
        text=text,content_sha256='a'*64))
    rows=[dict(original)]
    assert len(builder.repair_missing_margin_source_context(archive,rows))==1
    assert {e['date_iso'] for e in json.loads(rows[0]['temporary_end_evidence'])}=={'2026-06-15'}
    assert rows[0]['after']==original['after'] and rows[0]['before']==original['before']
    # Revocation of this notice, or an undated prior reference, stays a barrier.
    for unresolved in [text.replace('11503011071','11503011611'),
                       text.replace('115年5月28日','115年6月2日'),
                       text.replace('本公司115年5月28日','本公司')]:
        result=builder.candidate_notice_clock(unresolved,'2026-06-02')
        assert any(e['date_iso']=='2026-06-03' for e in json.loads(result['temporary_end_evidence']))


def test_referenced_restoration_binds_product_and_older_amount_not_current_before():
    text=('115年1月22日一般交易時段結束後，南亞科期貨契約(CYF)恢復為115年1月12日調整前保證金，'
          '晶豪科期貨契約(IIF)恢復為115年1月7日調整前保證金；'
          '華邦電期貨契約(FZF)於115年1月26日一般交易時段結束後恢復為115年1月12日調整前保證金。')
    row=dict(product='IIF',effective_date='2026-01-12',before=[.243,.1863,.18],
        after=[.324,.2484,.24],margin_kind='notional_rate',known_at='2026-01-09T23:59:59+08:00')
    prior=dict(row,effective_date='2026-01-07',before=[.162,.1242,.12],
        issue_date_bound=True,known_at='2026-01-06T23:59:59+08:00',source_content_sha256='a'*64)
    target=builder.referenced_before_restoration([text],row,[prior])
    assert target['restoration_target']==[.162,.1242,.12]
    assert target['restoration_rule']=='return_to_referenced_before'
    assert builder.referenced_before_restoration([text],row,[]) is None
    assert builder.referenced_before_restoration([text],dict(row,effective_date=None),[prior]) is None
    assert builder.referenced_before_restoration([text],dict(row,known_at=None),[prior]) is None
    assert builder.referenced_before_restoration([text],row,[dict(prior,known_at='2026-01-10T23:59:59+08:00')]) is None
    assert builder.referenced_before_restoration([text],row,[prior,dict(prior,before=[.135,.1035,.1])]) is None
    for product in ['CYF','FZF']:
        target=builder.referenced_before_restoration([text],dict(row,product=product),[prior])
        assert target['restoration_target']==row['before']


def test_shared_leading_margin_clock_keeps_each_named_before_reference():
    text=('本次保證金調整之商品為力積電期貨契約(QZF)及合晶期貨契約(PLF)。'
          '本次調整自115年6月3日一般交易時段結束後起實施。'
          '配合證券市場處置期間，115年6月15日一般交易時段結束後，'
          '力積電期貨恢復為115年6月3日調整前之保證金，'
          '合晶期貨恢復為115年5月29日調整前之保證金。')
    assigned=builder.named_margin_restoration_clauses(text)
    assert {next(iter(r['products'])):r['reference'] for r in assigned}=={
        'QZF':'115年6月3日','PLF':'115年5月29日'}
    assert {r['date_iso'] for r in assigned}=={'2026-06-15'}
    clock=dict(effective_date='2026-06-03',temporary_end_evidence='[]')
    for code in ['QZF','PLF']:
        scoped=builder.product_margin_restoration_clock(text,code,clock)
        ends=json.loads(scoped['temporary_end_evidence'])
        assert len(ends)==1 and ends[0]['product_scope']==code
    row=dict(product='QZF',effective_date='2026-06-03',before=[.2025,.1553,.15],
        margin_kind='notional_rate',known_at='2026-06-02T23:59:59+08:00')
    assert builder.referenced_before_restoration([text],row,[])==dict(
        restoration_target=row['before'],restoration_rule='return_to_declared_before')
    prior=dict(row,product='PLF',effective_date='2026-05-29',before=[.162,.1242,.12],
        issue_date_bound=True,source_content_sha256='a'*64)
    restored=builder.referenced_before_restoration([text],dict(row,product='PLF'),[prior])
    assert restored['restoration_target']==prior['before']
    assert restored['restoration_rule']=='return_to_referenced_before'
    assert builder.referenced_before_restoration([text],dict(row,product='PLF'),[]) is None
    for bad in [text.replace('合晶期貨恢復','未命名期貨恢復'),
                text+'另115年6月16日一般交易時段結束後恢復原保證金。',
                text.replace('合晶期貨契約(PLF)','合晶期貨契約(QZF)')]:
        assert builder.named_margin_restoration_clauses(bad) is None


def test_incomplete_margin_table_binds_issuing_context_without_overriding_complete_clocks(tmp_path):
    text = ('發文日期：中華民國112年1月30日。'
            '本次保證金金額並自112年1月31日一般交易時段結束後實施。')
    archive = SimpleNamespace(bundle=tmp_path, sources={}, document=lambda url:dict(
        text=text, content_sha256='a'*64))
    incomplete = dict(product='TX', effective_date=None, effective_phase='unresolved',
        issue_date_bound=False, published_date='2023-01-30', known_at='2023-01-30T23:59:59+08:00',
        before=[203000.,156000.,150000.], after=[184000.,141000.,136000.],
        margin_kind='fixed_twd', source_url='source', source_content_sha256='a'*64)
    complete = dict(incomplete, product='TF', effective_date='2023-01-31',
        effective_phase='after_product_regular_close', issue_date_bound=True,
        temporary_end_evidence='product-local')
    restoration = dict(incomplete, extraction='observed_disposition_conditional_restoration')
    rows = [dict(incomplete), dict(complete), dict(restoration)]
    result = builder.repair_missing_margin_source_context(archive, rows)
    assert len(result) == 1 and rows[0]['effective_date'] == '2023-01-31'
    assert rows[0]['effective_phase'] == 'after_product_regular_close'
    assert rows[0]['before'] == incomplete['before'] and rows[0]['after'] == incomplete['after']
    assert rows[1:] == [complete, restoration]
    # Distinct starts in the original remain ambiguous, with no partial mutation.
    archive.document = lambda url:dict(text=text +
        '另自112年2月1日一般交易時段結束後實施。', content_sha256='a'*64)
    unresolved = [dict(incomplete)]
    assert not builder.repair_missing_margin_source_context(archive, unresolved)
    assert unresolved == [incomplete]


def test_omitted_margin_end_requires_the_same_source_start_and_keeps_amounts(tmp_path):
    text=('發文日期：中華民國115年2月10日。'
          '本次實施期間自115年2月11日一般交易時段結束後起，'
          '預計至115年2月24日一般交易時段結束止。')
    archive=SimpleNamespace(bundle=tmp_path,sources={},document=lambda url:dict(
        text=text,content_sha256='a'*64))
    original=dict(product='TX',source_url='source',source_content_sha256='a'*64,
        published_date='2026-02-10',known_at='2026-02-10T23:59:59+08:00',
        issue_date_bound=True,effective_date='2026-02-11',
        effective_phase='after_product_regular_close',temporary_end_evidence='[]',
        requires_reversion_review=False,margin_kind='fixed_twd',
        after=[412000.,316000.,305000.],before=[374000.,287000.,277000.])
    rows=[dict(original)]
    repaired=builder.repair_missing_margin_source_context(archive,rows)
    assert len(repaired)==1 and rows[0]['requires_reversion_review']
    assert json.loads(rows[0]['temporary_end_evidence'])[0]['date_iso']=='2026-02-24'
    assert rows[0]['after']==original['after'] and rows[0]['before']==original['before']
    # A different source start cannot overwrite a complete product-local clock.
    archive.document=lambda url:dict(text=text.replace('115年2月11日','115年2月12日'),
                                     content_sha256='a'*64)
    unchanged=[dict(original)]
    assert not builder.repair_missing_margin_source_context(archive,unchanged)
    assert unchanged==[original]


def test_prior_notice_revocation_does_not_end_the_replacement_margin():
    text=('發文日期：中華民國111年4月19日。發文字號：台期結字第1110300646號。'
          '本次調整自111年4月20日該契約交易時段結束後起實施，'
          '並於111年5月3日該契約交易時段結束後恢復為111年4月15日調整前之保證金。'
          '四、原本公司於111年4月14日公告之台期結字第1110300611號函，'
          '自111年4月20日旨揭契約交易時段結束後停止適用。')
    clock=builder.candidate_notice_clock(text,'2022-04-19')
    assert clock['effective_date']=='2022-04-20'
    assert [e['date_iso'] for e in json.loads(clock['temporary_end_evidence'])]==['2022-05-03']
    proof=json.loads(clock['prior_notice_revocation_evidence'])
    assert proof[0]['revoked_notice_number']=='1110300611'
    assert proof[0]['prior_publication_date']=='2022-04-14'
    # Another clause ending this very notice on the same date stays an endpoint.
    current=builder.candidate_notice_clock(text+'本函自111年4月20日該契約交易時段結束後停止適用。',
                                          '2022-04-19')
    assert len(json.loads(current['temporary_end_evidence']))==2
    for changed in [text.replace('111年4月14日公告','111年4月19日公告'),
                    text.replace('1110300611','1110300646'),
                    text.replace('台期結字第1110300611號函','原公告'),
                    text.replace('於111年4月14日公告','於公告')]:
        unresolved=builder.candidate_notice_clock(changed,'2022-04-19')
        assert any(e['date_iso']=='2022-04-20' for e in json.loads(unresolved['temporary_end_evidence']))


def test_complete_margin_context_can_remove_only_the_source_proven_old_notice_end(tmp_path):
    from downloader.taifex_rule_parsing import temporal_mentions
    text=('發文日期：中華民國111年4月19日。'
          '本次調整自111年4月20日該契約交易時段結束後起實施，'
          '並於111年5月3日該契約交易時段結束後恢復為111年4月15日調整前之保證金。'
          '原本公司於111年4月14日公告之台期結字第1110300611號函，'
          '自111年4月20日旨揭契約交易時段結束後停止適用。')
    ends=[dict(e,boundary='after_regular_session') for e in temporal_mentions(text)
          if e['role']=='effective_end']
    row=dict(product='PDF',source_url='source',source_content_sha256='a'*64,
        published_date='2022-04-19',known_at='2022-04-19T23:59:59+08:00',
        issue_date_bound=True,effective_date='2022-04-20',effective_phase='after_product_regular_close',
        temporary_end_evidence=json.dumps(ends,ensure_ascii=False),requires_reversion_review=True,
        margin_kind='notional_rate',after=[.324,.2484,.24],before=[.243,.1863,.18])
    a=SimpleNamespace(bundle=tmp_path,sources={},document=lambda url:dict(text=text,content_sha256='a'*64))
    rows=[deepcopy(row)]
    assert len(builder.repair_missing_margin_source_context(a,rows))==1
    assert json.loads(rows[0]['temporary_end_evidence'])==[ends[0]]
    for key in ('after','before','known_at','effective_phase','source_content_sha256'):
        assert rows[0][key]==row[key]
    a.document=lambda url:dict(text=text.replace('111年4月14日公告','111年4月19日公告'),
                              content_sha256='a'*64)
    unchanged=[deepcopy(row)]
    assert not builder.repair_missing_margin_source_context(a,unchanged)
    assert unchanged==[row]


def test_reextraction_reuses_exact_observed_closure_postponement_once(tmp_path,monkeypatch):
    frame=tmp_path/'inputs.parquet';pl.DataFrame(dict(date=[date(2024,7,1)])).write_parquet(frame)
    universe=tmp_path/'universe.csv';pl.DataFrame(dict(product=['ODF'])).write_csv(universe)
    source=lambda p:dict(path=str(p),sha256=sha256_file(p))
    manifest=tmp_path/'inputs.json';manifest.write_text(json.dumps(dict(universe=source(universe),
        dispositions=source(frame),observations=source(frame),sources=[])))
    nominal=json.dumps([dict(date_iso='2024-07-24',boundary='after_regular_session')])
    actual=json.dumps([dict(date_iso='2024-07-26',boundary='after_regular_session')])
    row=dict(product='ODF',source_content_sha256='a'*64,source_url='law',
        known_at='2024-07-01T23:59:59+08:00',effective_date='2024-07-02',
        before=[.135,.1035,.1],requires_reversion_review=True,temporary_end_evidence=nominal)
    delay=dict(nominal_end='2024-07-24',actual_end='2024-07-26')
    restoration=dict(product='ODF',source_content_sha256='a'*64,
        original_rule_known_at=row['known_at'],
        restoration_evidence=json.dumps(dict(delayed_for_official_closure=delay)))
    a=SimpleNamespace(bundle=tmp_path,sources={},copy=lambda *args,**kwargs:None,
        document=lambda url:dict(content_sha256='a'*64,text='恢復為調整前之保證金'))
    validation_ends=[]
    def resolve(verified,*args,**kwargs):
        validation_ends.append([json.loads(r['temporary_end_evidence']) for r in verified])
        return [restoration],[]
    monkeypatch.setattr(builder,'disposal_margin_restorations',resolve)
    facts=[dict(row),dict(row,temporary_end_evidence=actual,original_temporary_end_evidence=nominal)]
    builder.verified_disposal_restorations(a,facts,manifest)
    assert all(json.loads(f['temporary_end_evidence'])[0]['date_iso']=='2024-07-26' for f in facts)
    builder.verified_disposal_restorations(a,facts,manifest)
    assert all(json.loads(f['original_temporary_end_evidence'])==json.loads(nominal) for f in facts)
    assert validation_ends[-1]==[json.loads(nominal),json.loads(nominal)]
    assert all(json.loads(f['temporary_end_evidence'])==json.loads(actual) for f in facts)
    tampered=[dict(facts[0],original_temporary_end_evidence='[]')]
    with pytest.raises(ValueError,match='lacks its nominal boundary'):
        builder.verified_disposal_restorations(a,tampered,manifest)
    unbound=[dict(row,temporary_end_evidence=actual)]
    with pytest.raises(ValueError,match='lacks its nominal boundary'):
        builder.verified_disposal_restorations(a,unbound,manifest)
    unrelated=[dict(row,temporary_end_evidence=json.dumps([dict(date_iso='2024-07-25')]))]
    with pytest.raises(ValueError,match='unrelated boundary'):
        builder.verified_disposal_restorations(a,unrelated,manifest)


@pytest.mark.parametrize('difference', [None, 'product', 'before', 'after', 'margin_kind',
    'effective_date', 'effective_phase', 'published_date', 'known_at', 'end_date',
    'end_phase', 'unbound_issue', 'unselected_instruction'])
def test_abbreviated_restoration_borrows_only_exact_verified_event_instruction(tmp_path, monkeypatch, difference):
    observations=tmp_path/'observations.parquet'
    pl.DataFrame(dict(symbol=['2327'],date=['2018-05-29'])).write_parquet(observations)
    dispositions=tmp_path/'dispositions.parquet'
    pl.DataFrame(dict(stock_id=['2327'],date=['2018-05-29'])).write_parquet(dispositions)
    universe=tmp_path/'universe.csv'
    pl.DataFrame(dict(product=['LXF'],underlying_symbol=['2327'])).write_csv(universe)
    source=lambda p:dict(path=str(p),sha256=sha256_file(p))
    manifest=tmp_path/'inputs.json';manifest.write_text(json.dumps(dict(universe=source(universe),
        dispositions=source(dispositions),observations=source(observations),sources=[])))
    formal_text=('國巨期貨(LXF)於107年5月28日恢復為107年5月16日調整前之保證金。'
        '調整期間如遇休市、有價證券停止買賣、全日暫停交易，則恢復日順延執行。')
    abbreviated='國巨期貨(LXF)恢復為調整前之保證金。'
    row=dict(product='LXF',source_url='formal',source_content_sha256='a'*64,
        published_date='2018-05-15',known_at='2018-05-15T23:59:59+08:00',issue_date_bound=True,
        effective_date='2018-05-16',effective_phase='after_product_regular_close',
        margin_kind='notional_rate',before=[.135,.1035,.1],after=[.2025,.1553,.15],
        requires_reversion_review=True,temporary_end_evidence=json.dumps([
            dict(date_iso='2018-05-28',boundary='after_regular_session')]))
    news=dict(row,source_url='news',source_content_sha256='b'*64)
    changes=dict(product='OTHER',before=[.162,.1242,.12],after=[.243,.1863,.18],
        margin_kind='fixed_twd',effective_date='2018-05-17',
        effective_phase='after_product_after_hours_close',published_date='2018-05-14',
        known_at='2018-05-15T20:00:00+08:00')
    if difference in changes:news[difference]=changes[difference]
    if difference in ('end_date','end_phase'):
        end=dict(date_iso='2018-05-28',boundary='after_regular_session')
        end['date_iso' if difference=='end_date' else 'boundary']=('2018-05-29'
            if difference=='end_date' else 'after_trading_session')
        news['temporary_end_evidence']=json.dumps([end])
    if difference=='unbound_issue':news['issue_date_bound']=False
    archive=SimpleNamespace(bundle=tmp_path,sources={},copy=lambda *args,**kwargs:None,
        document=lambda url:dict(content_sha256=('a' if url=='formal' else 'b')*64,
                                text=formal_text if url=='formal' else abbreviated))
    captured=[]
    def resolve(facts,*args,**kwargs):
        captured.extend(facts);return [],[]
    monkeypatch.setattr(builder,'disposal_margin_restorations',resolve)
    builder.verified_disposal_restorations(archive,[row,news],manifest,
        source_urls={'news'} if difference=='unselected_instruction' else {'formal','news'})
    verified=next(r for r in captured if r['source_url']=='news')
    if difference is None:
        assert verified['restoration_delay_rule']=='postpone_for_closed_or_full_day_halted_cash_sessions'
        proof=json.loads(verified['restoration_instruction_evidence'])
        assert proof['source_content_sha256s']==['a'*64] and proof['source_urls']==['formal']
        assert proof['known_at']==news['known_at']
    else:
        assert not verified.get('restoration_instruction_evidence')
        assert not verified.get('restoration_delay_rule')
    assert all(verified[k]==news[k] for k in ('before','after','known_at','source_content_sha256'))
    assert not news.get('restoration_instruction_evidence')


def test_restoration_uses_latest_named_subject_in_a_multi_product_notice():
    text=('中華民國109年3月24日公告宣德期貨契約(PPF)及為升期貨契約(ODF)。'
          '本次調高係宣德期貨契約(PPF)，4月8日恢復為調整前之保證金。'
          '本次為升期貨契約(ODF)調高，自109年3月25日起實施，4月8日恢復為3月20日調整前之保證金。')
    row=dict(product='ODF',effective_date='2020-03-25',before=[.243,.1863,.18],
        margin_kind='notional_rate',known_at='2020-03-24T23:59:59+08:00')
    prior=dict(row,effective_date='2020-03-20',before=[.162,.1242,.12],
        issue_date_bound=True,known_at='2020-03-19T23:59:59+08:00',source_content_sha256='a'*64)
    result=builder.referenced_before_restoration([text],row,[prior])
    assert result['restoration_target']==prior['before']
    result=builder.referenced_before_restoration([text],dict(row,product='PPF'),[prior])
    assert result['restoration_target']==row['before']
    # A connected list retains every explicitly named member across a sentence.
    text=('本次華新科期貨契約(HBF)、元太期貨契約(NVF)及金居期貨契約(PQF)，自115年5月18日起實施。'
          '115年5月28日恢復為115年5月18日調整前之保證金。'
          '三、本次延長之信昌電期貨契約(PKF)，恢復為115年5月13日調整前之保證金。')
    row=dict(row,effective_date='2026-05-18',known_at='2026-05-15T23:59:59+08:00')
    for product in ['HBF','NVF','PQF']:
        result=builder.referenced_before_restoration([text],dict(row,product=product),[])
        assert result['restoration_target']==row['before']


def test_partial_review_bundle_admits_only_independently_complete_documents(tmp_path):
    root=tmp_path/'ocr';folder=root/'documents'/('a'*64);folder.mkdir(parents=True)
    source=folder/'candidate.txt';source.write_text('完整原頁')
    proof=dict(url='source',content_sha256='a'*64,status='complete',pages=[dict(extraction='ocr_candidate')],
        files=[dict(path='candidate.txt',sha256=sha256_file(source))])
    (folder/'receipt.json').write_text(json.dumps(proof))
    manifest=dict(status='partial',point_in_time_verified=False,
        documents=[dict(content_sha256='a'*64,status='complete'),dict(content_sha256='b'*64,status='partial')],
        failures=[dict(content_sha256='c'*64,error='no text')],unprocessed_documents=2)
    (root/'manifest.json').write_text(json.dumps(manifest))
    archive=SimpleNamespace(copy=lambda *args,**kw:None,document=lambda url:dict(content_sha256='a'*64))
    result=builder.retain_review_override(archive,root)
    assert result['admitted_complete_documents']==1
    assert result['source_status']=='partial' and result['unprocessed_documents']==2
    assert result['skipped_partial_documents']==['b'*64]
    assert result['failed_documents']==['c'*64]
    source.write_text('changed')
    with pytest.raises(ValueError,match='SHA mismatch'):
        builder.retain_review_override(archive,root)


def test_single_session_clock_review_is_stock_only_dated_and_source_bound(tmp_path):
    universe = tmp_path/'products.csv'
    pl.DataFrame(dict(product=['ODF','TX','NYF'], underlying_security_type=['stock','index','etf'])).write_csv(universe)
    review = dict(review_kind='source_bound_single_regular_session', effective_date='2010-01-25',
        valid_until_exclusive='2024-01-22', known_at='2010-01-08T23:59:59+08:00',
        sources=[dict(source_url='law',content_sha256='a'*64,required_clauses=['日盤時段'])],
        universe=dict(path=str(universe),sha256=sha256_file(universe)),products=['ODF'])
    path=tmp_path/'review.json';path.write_text(json.dumps(review))
    a=SimpleNamespace(bundle=tmp_path, copy=lambda *args,**kw:None,
        document=lambda url:dict(content_sha256='a'*64,text='日盤時段'))
    row=dict(product='ODF',effective_date='2020-03-20',source_content_sha256='b'*64,
        effective_phase='after_product_trading_session_unspecified',
        temporary_end_evidence=json.dumps([dict(date_iso='2020-04-01',boundary='after_trading_session')]))
    rows=[dict(row),dict(row,product='TX'),dict(row,product='NYF'),dict(row,effective_date='2024-01-22')]
    assert len(builder.bind_single_session_margin_clocks(a,rows,path))==1
    assert rows[0]['effective_phase']=='after_product_regular_close'
    assert json.loads(rows[0]['temporary_end_evidence'])[0]['boundary']=='after_regular_session'
    assert all(r['effective_phase']=='after_product_trading_session_unspecified' for r in rows[1:])
    universe.write_text('modified')
    with pytest.raises(ValueError,match='scope SHA'):
        builder.bind_single_session_margin_clocks(a,[row],path)


@pytest.mark.parametrize('product', ['OEF', 'HBF', 'QSF'])
def test_common_close_restoration_reference_uses_own_annex_names(product):
    text = ('發文日期：中華民國115年6月4日。自115年6月5日一般交易時段結束後起實施。'
        '115年6月17日一般交易時段結束後，彩晶期貨恢復為115年6月2日標的證券未經處置前之保證金，'
        '華新科期貨恢復為115年5月18日標的證券未經處置前之保證金，'
        '小型南電期貨恢復為115年6月1日調整前之保證金。'
        '單位：比例(%)OEF(彩晶期貨)單位：比例(%)HBF(華新科期貨)'
        '單位：比例(%)LYF(南電期貨)單位：比例(%)QSF(小型南電期貨)')
    days = dict(OEF='2026-06-02', HBF='2026-05-18', QSF='2026-06-01')
    amounts = dict(OEF=[.135,.1035,.1], HBF=[.162,.1242,.12], QSF=[.216,.1656,.16])
    priors = [dict(product=p, effective_date=d, before=amounts[p], margin_kind='notional_rate',
        known_at='2026-05-01T23:59:59+08:00', issue_date_bound=True, source_content_sha256='a'*64)
        for p,d in days.items()]
    fact = dict(product=product, effective_date='2026-06-05', margin_kind='notional_rate', before=None,
        declared_non_disposed_margin=amounts[product], issue_date_bound=True, known_at='2026-06-04T23:59:59+08:00')
    result = builder.referenced_before_restoration([text],fact,priors)
    assert result['restoration_target'] == amounts[product]
    assert json.loads(result['restoration_reference_evidence'])['referenced_effective_date'] == days[product]
    assert builder.referenced_before_restoration([text],dict(fact, product='LYF'),priors) is None
    assert builder.referenced_before_restoration([text],dict(fact, declared_non_disposed_margin=[.243,.1863,.18]),priors) is None


def test_reextracted_clock_reapplies_retained_session_review_with_portable_scope(tmp_path):
    universe=tmp_path/'retained_scope.csv'
    pl.DataFrame(dict(product=['ODF','TX'],underlying_security_type=['stock','index'])).write_csv(universe)
    review=dict(review_kind='source_bound_single_regular_session',effective_date='2010-01-25',
        valid_until_exclusive='2024-01-22',known_at='2010-01-08T23:59:59+08:00',
        sources=[dict(source_url='law',content_sha256='a'*64,required_clauses=['日盤時段'])],
        universe=dict(path=str(tmp_path/'original_location/products.csv'),sha256=sha256_file(universe)),
        products=['ODF'])
    path=tmp_path/'review.json';path.write_text(json.dumps(review))
    a=SimpleNamespace(bundle=tmp_path,copy=lambda *args,**kw:None,
        document=lambda url:dict(content_sha256='a'*64,text='日盤時段'),sources={
            'review':dict(path=path.name,sha256=sha256_file(path),kind='single_session_rule_review'),
            'scope':dict(path=universe.name,sha256=sha256_file(universe),kind='single_session_product_scope')})
    row=dict(product='ODF',effective_date='2020-03-20',source_content_sha256='b'*64,
             effective_phase='after_product_trading_session_unspecified',temporary_end_evidence='[]')
    rows=[dict(row),dict(row,product='TX'),dict(row,effective_date='2024-01-22')]
    assert len(builder.bind_retained_single_session_margin_clocks(a,rows,path))==1
    assert rows[0]['effective_phase']=='after_product_regular_close'
    assert all(r['effective_phase']=='after_product_trading_session_unspecified' for r in rows[1:])
    path.write_text(json.dumps(dict(review,valid_until_exclusive='2025-01-01')))
    with pytest.raises(ValueError,match='review SHA mismatch'):
        builder.bind_retained_single_session_margin_clocks(a,[dict(row)])


def test_exact_named_close_survives_context_repair_but_needs_retained_session_proof(tmp_path):
    text=('臺灣期貨交易所新聞稿 中華民國104年8月20日。'
          '自104年8月21日該股票期貨契約交易時段結束後，開始實施。')
    row=dict(product='NFF',margin_kind='notional_rate',after=[.135,.1035,.1],before=[.2025,.1553,.15],
        source_url='original',source_content_sha256='b'*64,published_date='2015-08-20',
        known_at='2015-08-20T23:59:59+08:00',issue_date_bound=True,effective_date=None,
        effective_phase='unresolved',temporary_end_evidence='[]',requires_reversion_review=False)
    a=SimpleNamespace(bundle=tmp_path,sources={},copy=lambda *args,**kw:None,
        document=lambda url:dict(content_sha256='b'*64,text=text))
    rows=[deepcopy(row)]
    assert len(builder.repair_missing_margin_source_context(a,rows))==1
    assert rows[0]['effective_date']=='2015-08-21'
    assert rows[0]['effective_phase']=='after_product_trading_session_unspecified'
    assert not builder.margin_candidate_intervals(rows)[0]
    universe=tmp_path/'products.csv'
    pl.DataFrame(dict(product=['NFF'],underlying_security_type=['stock'])).write_csv(universe)
    review=dict(review_kind='source_bound_single_regular_session',effective_date='2010-01-25',
        valid_until_exclusive='2020-02-03',known_at='2010-01-08T23:59:59+08:00',
        sources=[dict(source_url='law',content_sha256='a'*64,required_clauses=['日盤時段'])],
        universe=dict(path=str(universe),sha256=sha256_file(universe)),products=['NFF'])
    path=tmp_path/'session_review.json';path.write_text(json.dumps(review))
    a.sources['review']=dict(path=path.name,sha256=sha256_file(path),kind='single_session_rule_review',url='')
    a.document=lambda url:dict(content_sha256='a'*64,text='日盤時段') if url=='law' else dict(
        content_sha256='b'*64,text=text)
    assert len(builder.bind_retained_single_session_margin_clocks(a,rows))==1
    assert rows[0]['effective_phase']=='after_product_regular_close'
    assert len(builder.margin_candidate_intervals(rows)[0])==1
    source=text+'並於104年9月1日該契約交易時段結束後恢復為調整前之保證金。'
    a.document=lambda url:dict(content_sha256='a'*64,text='日盤時段') if url=='law' else dict(
        content_sha256='b'*64,text=source)
    assert len(builder.repair_missing_margin_source_context(a,rows))==1
    ends=json.loads(rows[0]['temporary_end_evidence'])
    assert len(ends)==1 and ends[0]['date_iso']=='2015-09-01'
    assert ends[0]['boundary']=='after_regular_session'


def test_annex_requires_exact_dated_parent_and_matching_cover(tmp_path):
    a = archive(tmp_path, dict(annex='金額附件', notice='公告', cover=COVER), ['cover'])
    row = fact()
    changes = repair_position_source_context(a, [row], lives(), [])
    assert len(changes) == 1 and row['issue_date_bound']
    assert row['after'] == 8000 and row['effective_date'] == '2021-01-21'
    assert json.loads(row['publication_context_evidence'])['cover_content_sha256'] == 'cover-sha'
    # A second dated cover that contradicts the same annex leaves it blocked.
    a = archive(tmp_path, dict(annex='附件', notice='公告', cover=COVER,
        conflicting=COVER.replace('1月21日', '1月22日')), ['cover', 'conflicting'])
    row = fact()
    assert repair_position_source_context(a, [row], lives(), []) == []
    assert not row['issue_date_bound']
    a.conn.execute('DELETE FROM links WHERE child=?', ('conflicting',))
    a.conn.execute('UPDATE announcements SET published_date=?', ('2021-01-19',))
    assert repair_position_source_context(a, [fact()], lives(), []) == []


def test_relative_clause_requires_literal_unqualified_lowering_rule(tmp_path, monkeypatch):
    clause = ('調整本公司股票期貨部位限制，調降者，自公告日該期貨或該選擇權已上市之'
              '次近月份契約到期後次一營業日一般交易時段生效。正本：各期貨商')
    row = dict(fact(), published_date='2020-01-15', issue_date_bound=True,
               effective_date=None, direction='lower')
    a = archive(tmp_path, dict(annex=clause))
    before = deepcopy(row)
    assert repair_position_source_context(a, [row], lives(), [date(2020, 3, 19)])
    assert row['effective_date'] == '2020-03-19'
    assert row['after'] == before['after']
    monkeypatch.setattr(builder, 'retained_document_text_views', lambda *args:
        [(clause, 'verified_view'), (clause.replace('月份契約', '月份夫約'), 'incomplete_view')])
    repeated = deepcopy(before)
    assert repair_position_source_context(a, [repeated], lives(), [date(2020, 3, 19)])
    assert repeated['effective_date'] == row['effective_date']
    monkeypatch.undo()
    a = archive(tmp_path, dict(annex=clause.replace('調降者', '除HSF外調降者')))
    assert repair_position_source_context(a, [before], lives(), [date(2020, 3, 19)]) == []
    assert before['effective_date'] is None
    with pytest.raises(ValueError, match='candidate bytes'):
        repair_position_source_context(a, [dict(before, source_content_sha256='wrong')], lives(), [])


@pytest.mark.parametrize('separator',['','，',','])
def test_named_restoration_comma_repairs_only_its_exact_product_clock(tmp_path,separator):
    text=('發文日期：中華民國115年8月3日。'
          '本次調整之商品為南電期貨契約(LYF)、小型南電期貨契約(QSF)及合晶期貨契約(PLF)。'
          '本次調整自115年8月4日一般交易時段結束後起實施。'
          f'南電期貨及小型南電期貨於115年8月18日一般交易時段結束後{separator}'
          '恢復為115年8月4日調整前之保證金；'
          f'合晶期貨於115年8月14日一般交易時段結束後{separator}'
          '恢復為115年7月27日調整前之保證金。')
    original=dict(product='LYF',source_url='source',source_content_sha256='a'*64,
        published_date='2026-08-03',known_at='2026-08-03T23:59:59+08:00',issue_date_bound=True,
        effective_date='2026-08-04',effective_phase='after_product_regular_close',
        temporary_end_evidence=json.dumps([dict(date_iso=day,boundary='after_regular_session')
            for day in ['2026-08-14','2026-08-18']]),requires_reversion_review=True,
        margin_kind='notional_rate',before=[.216,.1656,.16],after=[.324,.2484,.24])
    archive=SimpleNamespace(bundle=tmp_path,sources={},document=lambda url:dict(
        text=text,content_sha256='a'*64))
    rows=[dict(original,product=product) for product in ['LYF','QSF','PLF']]
    assert len(builder.repair_missing_margin_source_context(archive,rows))==3
    for row,day in zip(rows,['2026-08-18','2026-08-18','2026-08-14'],strict=True):
        assert {e['date_iso'] for e in json.loads(row['temporary_end_evidence'])}=={day}
        assert row['restoration_clock_scope']=='explicit_product_clause'
        assert row['before']==original['before'] and row['after']==original['after']
        assert row['known_at']==original['known_at'] and row['source_content_sha256']=='a'*64
    # A partly named or conflicting notice cannot repair just the recognized
    # half or choose one of its dates by row order.
    for ambiguous in [text.replace('合晶期貨於','未知期貨於'),
                      text+'南電期貨於115年8月19日一般交易時段結束後恢復為調整前保證金。']:
        archive.document=lambda url:dict(text=ambiguous,content_sha256='a'*64)
        unchanged=[dict(original)]
        assert not builder.repair_missing_margin_source_context(archive,unchanged)
        assert unchanged==[original]


@pytest.mark.parametrize('review_layout',['single','collection'])
def test_after_only_restoration_uses_exact_retained_review_and_prior_amount(tmp_path,monkeypatch,review_layout):
    from downloader.artifact_io import sha256_file
    text=('商品為昇佳期貨(SAF)。昇佳期貨於114年10月7日一般交易時段結束後，'
          '恢復為114年10月1日標的證券未經處置前之保證金。')
    review=dict(source_url='official',content_sha256='a'*64,published_date='2025-10-02',transcribed_text=text)
    path=tmp_path/'review.json';path.write_text(json.dumps(
        dict(reviews=[review]) if review_layout=='collection' else review))
    proof={}
    for key in ['universe','dispositions','observations']:
        file=tmp_path/(key+('.csv' if key=='universe' else '.parquet'))
        frame=pl.DataFrame(dict(product=['SAF'],underlying_symbol=['1795']) if key=='universe'
            else {'stock_id' if key=='dispositions' else 'symbol':['1795']})
        if key=='universe':frame.write_csv(file)
        else:frame.write_parquet(file)
        proof[key]=dict(path=str(file),sha256=sha256_file(file))
    proof['sources']=[]
    inputs=tmp_path/'inputs.json';inputs.write_text(json.dumps(proof))
    sources={path.name:dict(kind='visual_source_review',url='official',path=path.name,sha256=sha256_file(path))}
    a=SimpleNamespace(bundle=tmp_path,sources=sources,copy=lambda *args,**kwargs:None,
        document=lambda url:dict(content_sha256='a'*64,text='separate unscoped footer'))
    prior=dict(product='SAF',effective_date='2025-10-01',margin_kind='notional_rate',
        before=[.135,.1035,.1],issue_date_bound=True,source_content_sha256='b'*64,
        known_at='2025-09-30T23:59:59+08:00')
    row=dict(product='SAF',source_url='official',source_content_sha256='a'*64,
        published_date='2025-10-02',known_at='2025-10-02T23:59:59+08:00',
        effective_date='2025-10-03',margin_kind='notional_rate',before=None,after=[.27,.207,.2],
        issue_date_bound=True,requires_reversion_review=True,temporary_end_evidence='[]',
        extraction='source_bound_visual_transcription',source_review_clock_text=text,
        visual_review_sha256=sha256_file(path))
    captured=[]
    def resolve(facts,*args,**kwargs):
        captured.extend(facts);return [],[]
    monkeypatch.setattr(builder,'disposal_margin_restorations',resolve)
    assert not builder.verified_disposal_restorations(a,[prior,row],inputs)
    verified=captured[-1]
    assert verified['before'] is None and verified['after']==row['after']
    assert verified['restoration_target']==prior['before']
    assert verified['restoration_rule']=='return_to_referenced_before'
    assert json.loads(verified['restoration_reference_evidence'])['source_content_sha256s']==['b'*64]
    assert 'restoration_target' not in row
    other=dict(row,source_url='other',source_content_sha256='c'*64,
               source_review_clock_text=None,visual_review_sha256=None)
    captured.clear()
    assert not builder.verified_disposal_restorations(a,[prior,row,other],inputs,source_urls={'official'})
    assert captured[-1]['requires_reversion_review'] is False
    assert captured[-1]['after']==other['after'] and other['requires_reversion_review'] is True
    for change in [dict(source_review_clock_text=text.replace('10月1日','10月2日')),
                   dict(visual_review_sha256=None)]:
        with pytest.raises(ValueError,match='review context'):
            builder.verified_disposal_restorations(a,[prior,dict(row,**change)],inputs)


def test_scoped_restoration_rebuild_keeps_other_verified_history(monkeypatch):
    before=dict(source_url='owned',effective_date='2026-08-04',after=[.324,.2484,.24])
    owned=dict(before,extraction='observed_disposition_conditional_restoration',effective_date='2026-08-18')
    unrelated=dict(source_url='historical',effective_date='2024-11-14',after=[.135,.1035,.1],
        restoration_evidence='retained completed 10-session proof',
        extraction='observed_disposition_conditional_restoration')
    original=[before,owned,unrelated]
    def resolve(archive,facts,path,*,source_urls=None):
        assert source_urls=={'owned'} and unrelated in facts and owned not in facts
        return [dict(owned,restoration_evidence='new owned-source proof')]
    monkeypatch.setattr(builder,'verified_disposal_restorations',resolve)
    result=builder.rebuild_disposal_restorations(None,original,None,source_urls={'owned'})
    assert result[1] is unrelated and result[-1]['restoration_evidence']=='new owned-source proof'
    assert original==[before,owned,unrelated]


@pytest.mark.parametrize('problem', [None, 'missing_session', 'different_product', 'different_before',
    'different_kind', 'opening_phase', 'same_day_margin', 'late_publication', 'unbound_issue',
    'different_period_end', 'earlier_overlap', 'different_notice_day', 'source_sha',
    'conflicting_successor', 'intervening_amount', 'no_own_restoration'])
def test_independent_cash_disposition_does_not_extend_the_previous_futures_margin(problem):
    from stockagent.data.tw_futures_margin_preparation import disposal_margin_restorations, margin_candidate_intervals
    fact = dict(product='AAF', margin_kind='notional_rate', before=[.2,.15,.14], after=[.3,.23,.22],
        effective_date='2023-07-26', effective_phase='after_product_regular_close',
        published_date='2023-07-25', known_at='2023-07-25T23:59:59+08:00', issue_date_bound=True,
        source_content_sha256='a'*64, source_url='https://www.taifex.com.tw/old.pdf',
        requires_reversion_review=True, restoration_rule='return_to_declared_before',
        temporary_end_evidence=json.dumps([dict(date_iso='2023-08-07',boundary='after_regular_session')]))
    successor = dict(fact, before=fact['before'], after=[.4,.3,.28], effective_date='2023-08-08',
        published_date='2023-08-07', known_at='2023-08-07T23:59:59+08:00',
        source_content_sha256='e'*64, source_url='https://www.taifex.com.tw/new.pdf',
        requires_reversion_review=False, temporary_end_evidence=json.dumps([
            dict(date_iso='2023-08-21',boundary='after_regular_session')]))
    own = dict(date='2023-07-24',stock_id='1000',period_start='2023-07-25',period_end='2023-08-07',
        measure='處置期間（十個營業日）',source_sha256='b'*64)
    overlap = dict(own,date='2023-08-04',period_start='2023-08-07',period_end='2023-08-21',source_sha256='d'*64)
    changes = dict(different_product=dict(product='BBF'), different_before=dict(before=[.21,.16,.15]),
        different_kind=dict(margin_kind='fixed_twd'), opening_phase=dict(effective_phase='product_regular_open'),
        same_day_margin=dict(effective_date='2023-08-07'),
        late_publication=dict(known_at='2023-08-08T00:00:00+08:00'), unbound_issue=dict(issue_date_bound=False),
        different_period_end=dict(temporary_end_evidence=json.dumps([
            dict(date_iso='2023-08-22',boundary='after_regular_session')])))
    successor.update(changes.get(problem,{}))
    if problem=='earlier_overlap':overlap['period_start']='2023-08-04'
    if problem=='no_own_restoration':fact.pop('restoration_rule')
    facts=[fact,successor]
    if problem=='conflicting_successor':facts.append(dict(successor,after=[.45,.34,.32],source_content_sha256='f'*64))
    if problem=='intervening_amount':facts.append(dict(successor,effective_date='2023-08-04'))
    days=['2023-07-25','2023-07-26','2023-07-27','2023-07-28','2023-07-31',
        '2023-08-01','2023-08-02','2023-08-03','2023-08-04','2023-08-07','2023-08-08']
    if problem=='missing_session':days.remove('2023-08-02')
    obs=pl.DataFrame(dict(date=days,symbol=['1000']*len(days),volume=[1.]*len(days),source_sha256=['c'*64]*len(days)))
    dispositions=pl.DataFrame([own,overlap])
    universe=pl.DataFrame(dict(product=['AAF','BBF'],underlying_symbol=['1000','2000']))
    rows,issues=disposal_margin_restorations(facts,universe,dispositions,obs)
    assert not rows
    text='發文日期：中華民國112年8月7日。'
    if problem=='different_notice_day':text='發文日期：中華民國112年8月8日。'
    archive=SimpleNamespace(document=lambda url:dict(content_sha256='0'*64 if problem=='source_sha'
        else 'e'*64 if url.endswith('new.pdf') else 'a'*64,text=text))
    if problem=='source_sha':
        with pytest.raises(ValueError,match='successor source SHA mismatch'):
            builder.restore_independent_boundary_dispositions(archive,facts,universe,dispositions,obs,rows,issues)
        return
    restored,remaining=builder.restore_independent_boundary_dispositions(
        archive,facts,universe,dispositions,obs,rows,issues)
    if problem:
        assert not restored
        assert remaining or problem=='no_own_restoration'
        return
    assert len(restored)==1 and not remaining
    assert restored[0]['after']==fact['before'] and restored[0]['before']==fact['after']
    assert restored[0]['known_at']=='2023-08-07T13:35:00+08:00'
    proof=json.loads(restored[0]['restoration_evidence'])
    assert proof['completed_cash_dates']==days[:-1]
    separate=proof['independent_boundary_disposition']
    assert separate['excluded_disposition_rows']==[overlap]
    assert separate['restoration_target']==fact['before'] and separate['point_in_time_verified'] is False
    assert separate['successor_sources'][0]['known_at']==successor['known_at']
    levels,errors=margin_candidate_intervals([fact,*restored,successor])
    assert not errors
    gap_day=next(r for r in levels if r['effective_date']=='2023-08-07')
    assert gap_day['initial']==.2 and gap_day['valid_until_date_exclusive']=='2023-08-08'
    assert gap_day['source_content_sha256s']==['a'*64]
    assert dispositions.height==2 and facts==[fact,successor]


@pytest.mark.parametrize('problem', ['status', 'parent', 'worklist', 'output_sha', 'escaped_source', 'product_scope'])
def test_bounded_margin_continuation_rejects_unbound_seed_and_scope(tmp_path, monkeypatch, problem):
    import scripts.repair_tw_futures_margin_source_intervals as repair
    from downloader.artifact_io import atomic_write_json
    current=tmp_path/'current';current.mkdir()
    atomic_write_json(current/'manifest.json',dict(identity='retained_parent'))
    worklist=tmp_path/'gaps.csv'
    pl.DataFrame(dict(product=['AAF'],missing_opening_margin=[True],missing_settlement_margin=[True])).write_csv(worklist)
    seed=tmp_path/'pending';seed.mkdir()
    outputs={}
    for name in ['margin_event_candidates.parquet','margin_level_intervals.parquet','margin_interval_issues.json']:
        if name.endswith('.parquet'):pl.DataFrame(dict(product=['AAF'])).write_parquet(seed/name)
        else:atomic_write_json(seed/name,[])
        outputs[name]=dict(sha256=sha256_file(seed/name))
    proof=dict(status='bounded_margin_restoration_sources_prepared',
        parent_manifest_sha256=sha256_file(current/'manifest.json'),
        worklist_sha256=sha256_file(worklist),outputs=outputs,sources=[])
    if problem=='status':proof['status']='unverified'
    if problem=='parent':proof['parent_manifest_sha256']='a'*64
    if problem=='worklist':proof['worklist_sha256']='b'*64
    if problem=='output_sha':proof['outputs']['margin_event_candidates.parquet']['sha256']='c'*64
    if problem=='escaped_source':proof['sources']=[dict(path='../outside.json',sha256='d'*64,url='',kind='raw')]
    atomic_write_json(seed/'manifest.json',proof)
    monkeypatch.setattr(repair,'read_bound_output',lambda path:(pl.DataFrame(dict(product=['AAF'])),dict(sources=[])))
    expected=('source/worklist parent' if problem in ('status','parent','worklist') else
        'output SHA mismatch' if problem=='output_sha' else 'escapes its bundle' if problem=='escaped_source'
        else 'exceed the retained gap scope')
    with pytest.raises(ValueError,match=expected):
        repair.prepare_margin_repairs(current,worklist,tmp_path/'output',tmp_path/'receipt.json',
            archive_root=tmp_path/'not_opened',selected_products=['BBF'] if problem=='product_scope' else ['AAF'],
            continue_delta=seed)
    assert not (tmp_path/'output').exists() and sha256_file(seed/'manifest.json')


def grade_regime_case(tmp_path):
    from downloader.artifact_io import atomic_write_json
    conn=sqlite3.connect(':memory:')
    conn.row_factory=sqlite3.Row
    conn.execute('CREATE TABLE announcements(url TEXT,published_date TEXT)')
    docs={};sources={};proofs={}
    for name,digest,published,pages in [('original','a'*64,'2012-04-19',[1,2]),
            ('law','b'*64,'2013-02-20',[1,26,27,28,29,30]),
            ('replacement','c'*64,'2013-02-20',[1,2,3,4,5])]:
        conn.execute('INSERT INTO announcements VALUES (?,?)',(name,published))
        docs[name]=dict(content_sha256=digest,text='')
        count={'original':2,'law':55,'replacement':5}[name]
        receipt=tmp_path/f'{name}-receipt.json'
        atomic_write_json(receipt,dict(url=name,content_sha256=digest,status='complete',
            document_pages=count,extracted_pages=count,pages=[dict(page=n) for n in range(1,count+1)]))
        images=[]
        for page in pages:
            image=tmp_path/f'{name}-{page}.png';image.write_bytes(f'{name} page {page}'.encode())
            images.append(dict(page=page,path=image.name,sha256=sha256_file(image)))
        text=('發文日期：中華民國101年4月19日。' if name=='original' else
            '發文日期：中華民國102年2月20日。')
        proofs[name]=dict(source_url=name,content_sha256=digest,published_date=published,
            transcribed_text=text,complete_original_receipt=receipt.name,
            complete_original_receipt_sha256=sha256_file(receipt),pages=images)
        if name!='law':
            numeric=tmp_path/f'{name}-numeric.json'
            atomic_write_json(numeric,dict(reviews=[dict(source_url=name,content_sha256=digest,published_date=published)]))
            proofs[name]['numeric_review_sha256']=sha256_file(numeric)
            sources[name]=dict(path=numeric.name,sha256=sha256_file(numeric),url='',kind='visual_position_cell_review')
    proofs['law'].update(transcribed_text=proofs['law']['transcribed_text']+
        '台期交字第10202001810號。股票期貨契約交易規則，第十六條，除本公司另有規定外，並自102年2月20日起實施。',
        old_natural_person_grade_limits=[5000,3750,2500,1250,350],new_natural_person_grade_limits=[8000,4000,2000])
    proofs['replacement']['transcribed_text']+='公告放寬本公司股票期貨交易人部位限制數，並自102年2月20日起實施。'
    review=dict(review_kind='source_bound_stock_futures_grade_regime_replacement_v1',
        effective_date='2013-02-20',replacement_is_complete_futures_roster=True,**proofs,
        rows=[dict(product='DPF',underlying_symbol='2892',old_natural_person_limit=3750.,
            old_effective_date='2013-03-21',new_natural_person_limit=4000.,
            new_effective_date='2013-02-20',named_listing_exception=False)])
    old=dict(product='DPF',underlying_symbol='2892',effective_date='2013-03-21',
        natural_person_limit=3750.,event_type='absolute_level',unit='contracts',
        effective_phase='product_regular_open',source_url='original',source_content_sha256='a'*64,
        known_at='2012-04-19T23:59:59+08:00',published_date='2012-04-19',issue_date_bound=True,
        visual_review_sha256=proofs['original']['numeric_review_sha256'])
    new=dict(old,effective_date='2013-02-20',natural_person_limit=4000.,source_url='replacement',
        source_content_sha256='c'*64,known_at='2013-02-20T23:59:59+08:00',published_date='2013-02-20',
        effective_phase='date_only_requires_phase_review',futures_eligible=True,
        visual_review_sha256=proofs['replacement']['numeric_review_sha256'])
    class Archive:
        bundle=tmp_path
        def __init__(self):self.conn=conn;self.sources=sources
        def document(self,url):return docs[url]
        def copy(self,path,digest,**kwargs):assert sha256_file(path)==digest
    path=tmp_path/'review.json';atomic_write_json(path,dict(reviews=[review]))
    return Archive(),[old,new],review,path


@pytest.mark.parametrize('problem',[None,'wrong_source','incomplete_original','missing_law_page',
    'missing_numeric_review','wrong_new_cap','other_ticker','already_effective','named_exception',
    'not_futures','new_regime_cap','later_roster'])
def test_dated_three_grade_law_requires_both_numeric_originals_and_full_roster(tmp_path,problem):
    a,rows,review,path=grade_regime_case(tmp_path)
    original=deepcopy(rows)
    if problem=='wrong_source':review['law']['content_sha256']='d'*64
    if problem=='incomplete_original':
        receipt=tmp_path/review['law']['complete_original_receipt'];payload=json.loads(receipt.read_text())
        payload['extracted_pages']=54;receipt.write_text(json.dumps(payload))
        review['law']['complete_original_receipt_sha256']=sha256_file(receipt)
    if problem=='missing_law_page':review['law']['pages']=review['law']['pages'][:-1]
    if problem=='missing_numeric_review':a.sources.clear()
    if problem=='wrong_new_cap':rows[1]['natural_person_limit']=8000.
    if problem=='other_ticker':rows[1]['underlying_symbol']='2330'
    if problem=='already_effective':review['rows'][0]['old_effective_date']='2013-02-19'
    if problem=='named_exception':review['rows'][0]['named_listing_exception']=True
    if problem=='not_futures':rows[1]['futures_eligible']=False
    if problem=='new_regime_cap':review['rows'][0]['old_natural_person_limit']=2000.
    if problem=='later_roster':review['replacement']['published_date']='2013-02-21'
    if problem:
        with pytest.raises(ValueError):builder.apply_stock_futures_grade_regime_review(a,rows,review,path)
        return
    result=builder.apply_stock_futures_grade_regime_review(a,rows,review,path)
    assert rows==original and result[0]['natural_person_limit']==3750.
    assert result[0]['effective_date']=='2013-03-21' and result[0]['notice_revoked_effective_date']=='2013-02-20'
    from stockagent.data.tw_futures_margin_preparation import position_candidate_intervals
    current,issues=position_candidate_intervals(result)
    assert len(current)==1 and current[0]['position_limit']==4000.
    assert current[0]['admission_not_before']=='2013-02-20T23:59:59+08:00'
    assert current[0]['source_content_sha256s']==['a'*64,'b'*64,'c'*64]
    assert any(i['reasons']=='notice_revoked_before_scheduled_effective_date' for i in issues)


def test_future_grade_regime_law_does_not_rewrite_original_as_of_arithmetic(tmp_path):
    a,rows,review,path=grade_regime_case(tmp_path)
    amended=builder.apply_stock_futures_grade_regime_review(a,rows,review,path)
    original=deepcopy(amended)
    before=builder._position_grade_levels_known_at(amended,'DPF','2012-07-30T23:59:59+08:00')
    after=builder._position_grade_levels_known_at(amended,'DPF','2013-07-24T23:59:59+08:00')
    assert len(before)==1 and before[0]['position_limit']==3750.
    assert before[0]['effective_date']=='2013-03-21'
    assert len(after)==1 and after[0]['position_limit']==4000.
    assert amended==original
