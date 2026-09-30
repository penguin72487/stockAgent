from datetime import date
import json
import sqlite3
from types import SimpleNamespace

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from stockagent.data.tw_futures_margin_preparation import (
    align_rules, check_publication, event_at, margin_effective, margin_pdf_values,
    position_change, prepare_daily, validate_event_chain, verified_margin_values,
    reviewed_legacy_events, margin_legacy_word_levels,
)


PDF = '''單位：新臺幣元
TX
調整後保證金金額 調整前保證金金額
原始保證金金額 維持保證金金額 結算保證金金額
原始保證金金額 維持保證金金額 結算保證金金額
保證金 184,000 141,000 136,000 167,000 128,000 123,000
'''


def test_margin_requires_product_local_currency_and_order():
    assert margin_pdf_values(PDF, 'TX') == (184000, 141000, 136000, 167000, 128000, 123000)
    for text in [PDF.replace('新臺幣', '美元'), PDF.replace('TX\n', 'MTX\n'),
                 PDF.replace('調整後保證金金額 調整前保證金金額', '調整前保證金金額 調整後保證金金額')]:
        with pytest.raises(ValueError):
            margin_pdf_values(text, 'TX')


def test_csv_pdf_crosscheck_does_not_trust_reused_url():
    values = (184000, 141000, 136000, 167000, 128000, 123000)
    headers = [phase + kind + '保證金' for phase in ['調整後', '調整前'] for kind in ['原始','維持','結算']]
    row = dict(zip(headers, map(str, values)), 契約代碼='TX', 契約中文簡稱='臺股期貨')
    assert verified_margin_values([row], PDF, 'TX') == values
    row['調整後原始保證金'] = '701000'
    with pytest.raises(ValueError, match='disagreement'):
        verified_margin_values([row], PDF, 'TX')
    with pytest.raises(ValueError, match='incomplete'):
        verified_margin_values([row, row], PDF, 'TX')


def test_margin_vertical_header_labels_are_not_the_numeric_row():
    from stockagent.data.tw_futures_margin_preparation import margin_table_candidates
    text = ('單位：比例(%)\nCXF\n(大同期貨)\n調整後保證金適用比例\n(級距2)\n'
            '調整前保證金適用比例\n(級距1)\n' +
            '\n'.join(['原始\n保證金', '維持\n保證金', '結算\n保證金'] * 2) +
            '\n保證金 16.20% 12.42% 12.00% 13.50% 10.35% 10.00%\n')
    rows = margin_table_candidates(text)
    assert len(rows) == 1 and rows[0]['product'] == 'CXF'
    assert rows[0]['after'] == [0.162, 0.1242, 0.12]
    assert rows[0]['before'] == [0.135, 0.1035, 0.1]
    assert margin_table_candidates(text.replace('13.50%', '13.50')) == []
    assert margin_table_candidates(text.replace('CXF', 'CXO')) == []
    assert margin_table_candidates(text.replace('維持\n保證金', '原始\n保證金', 1)) == []


def test_holiday_effective_clause_and_attachment_date():
    assert margin_effective('自112年1月17日一般交易時段結束後起，預計於春節後恢復。') == '2023-01-17T13:45:00+08:00'
    check_publication('發文日期：中華民國112年1月16日', '2023-01-16')
    check_publication('發文日期：中華民國九十三年五月二十六日', '2004-05-27')
    with pytest.raises(ValueError):
        check_publication('發文日期：中華民國九十三年五月二十六日', '2005-05-27')
    with pytest.raises(ValueError):
        check_publication('發文日期：中華民國115年1月16日', '2023-01-16')
    assert margin_effective('自100年9月8日交易時段結束後起實施') == '2011-09-08T13:45:00+08:00'
    with pytest.raises(ValueError, match='regular close'):
        margin_effective('自107年9月8日交易時段結束後起實施')


def test_legacy_word_levels_keep_rows_and_futures_option_columns_distinct():
    text = ('結算保證金適用比例 |維持保證金適用比例 |原始保證金適用比例 | |'
            '1 |DSF |宏碁期貨 |宏碁股份有限公司 |級距1 |10.00% |10.35% |13.50% | |'
            '2 |DTF |勝華期貨 |勝華科技股份有限公司 |級距2 |12.00% |12.42% |16.20% | |'
            '3 |DSO |宏碁選擇權 |宏碁股份有限公司 |級距1 |10.00% |10.35% |13.50% | |註：其他')
    rows = margin_legacy_word_levels(text)
    assert [r['product'] for r in rows] == ['DSF','DTF']
    assert rows[0]['after'] == [0.135,0.1035,0.1]
    assert rows[1]['after'] == [0.162,0.1242,0.12]
    assert [r['product'] for r in margin_legacy_word_levels(text.replace('13.50%','13.50'))] == ['DTF']
    assert margin_legacy_word_levels(text.replace('結算保證金適用比例','原始保證金適用比例')) == []


def test_legacy_fixed_margin_keeps_chinese_amounts_and_unknown_currency():
    text='''|商品別|調整後保證金金額|調整前保證金金額|
| |原始|維持|結算|原始|維持|結算|
|TX|十萬五千元|八萬一千元|七萬元|十二萬元|九萬二千元|八萬元|
|MTX|2萬7千元|2萬1千元|1萬8千元|3萬元|2萬3千元|2萬元|
|TXO|三萬元|二萬元|一萬元|三萬元|二萬元|一萬元|
'''
    rows=margin_legacy_word_levels(text)
    assert [r['product'] for r in rows]==['TX','MTX']
    assert rows[0]['after']==[105000,81000,70000]
    assert rows[0]['before']==[120000,92000,80000]
    assert rows[1]['after']==[27000,21000,18000]
    assert all(r['margin_kind']=='fixed_unresolved_currency' for r in rows)
    assert all(r['candidate_only'] for r in rows)
    wrapped='''|商品別|原始|維持|結算|
| |保證金|保證金|保證金|
|T5F|四萬|三萬|三萬元|
| |五千元|五千元| |
'''
    assert margin_legacy_word_levels(wrapped)[0]['after']==[45000,35000,30000]
    assert margin_legacy_word_levels(wrapped.replace('五千元','每口五千元'))==[]


def test_legacy_positions_never_confuse_grade_or_corporate_limit():
    from stockagent.data.tw_futures_margin_preparation import position_legacy_word_candidates
    text='''股票期貨 部位限制契約數 自然人 法人
|CAF|南亞期貨|4|1,250|3,750|
|CBF|中鋼期貨|1|5,000|15,000|
|CAO|南亞選擇權|4|1,250|3,750|
1 |宏碁股份有限公司 |DSF |宏碁期貨 |DSO |宏碁選擇權 |2,500 |7,500 |18,750 | |2 |勝華科技股份有限公司 |DTF |勝華期貨 |DTO |勝華選擇權 |5,000 |15,000 |37,500 |
'''
    rows=position_legacy_word_candidates(text)
    assert [(r['product'],r['natural_person_limit']) for r in rows]==[
        ('CAF',1250),('CBF',5000),('DSF',2500),('DTF',5000)]
    assert all(r['candidate_only'] and r['effective_date'] is None for r in rows)
    assert position_legacy_word_candidates(text.replace('自然人','法人'))==[]


def test_native_launch_position_table_preserves_combined_mini_formula():
    from stockagent.data.tw_futures_margin_preparation import position_grid_candidates
    rows=[['標的證券代碼','契約英文代碼','中文簡稱','部位限制數(單位：契約數)','標準型證券股數/受益權單位'],
          [None,None,None,'自然人',None],
          ['1519','VM','華城期貨','2,000','2,000'],
          ['1519','VN','小型華城期貨','與華城期貨合併計算(依20口小型華城期貨等於1口華城期貨合併計算)','100']]
    facts=position_grid_candidates(rows,'新增股票期貨')
    assert len(facts)==2
    assert facts[0]['natural_person_limit']==2000
    assert facts[1]['natural_person_limit'] is None
    assert facts[1]['combined_position_ratio']=='1/20'
    assert facts[1]['combined_position_base_name']=='華城期貨'
    assert facts[1]['underlying_symbol']=='1519'
    assert facts[1]['contract_multiplier']==100
    assert facts[1]['candidate_only'] and facts[1]['effective_date'] is None


def test_historical_limit_boundary_and_expiry_phase():
    frame = pl.DataFrame(dict(date=[date(2015,5,29),date(2015,6,1)],
        physical_contract=['TX:201506']*2, product=['TX']*2, asset_class=['index_future']*2,
        previous_settlement=[9000.]*2, liquidation_reason=['carry_same_contract','last_trade_date']))
    event = dict(after=[83000,64000,61000], before=None, known_at='2014-01-01T23:59:59+08:00',
        effective_at='2014-01-02T13:45:00+08:00', source_url='verified')
    rules=align_rules(frame,dict(TX=[event]),[dict(event,after=6000)])
    assert rules['upper_limit'].to_list() == [9630.,9900.]
    assert rules['lower_limit'].to_list() == [8370.,8100.]
    assert rules['settlement_time'].to_list() == ['13:45:00','13:30:00']


def legacy_position_archive(after=6000, digest='a'*64):
    conn=sqlite3.connect(':memory:'); conn.row_factory=sqlite3.Row
    conn.execute('CREATE TABLE announcements(url TEXT,published_date TEXT)')
    notices=[]
    for i,pub,eff,amount,immediate in [(0,'2010-10-13','2010-11-18',5000,False),
                                     (1,'2011-04-08','2011-04-07',after,True)]:
        url=f'https://www.taifex.com.tw/{i}.pdf'
        conn.execute('INSERT INTO announcements VALUES (?,?)',(url,pub))
        notices.append(dict(url=url,announcement_url=url,published_date=pub,issued_date=pub if not immediate else eff,
            effective_date=eff,kind='position',after=amount,immediate=immediate,
            content_sha256=digest,pages_one_based=[1]))
    return SimpleNamespace(conn=conn,legacy=dict(verified_start='2011-01-03',notices=notices),
        children=lambda u:[],document=lambda u:dict(content_sha256='a'*64))


def test_late_known_relaxation_retains_stricter_limit_but_tightening_fails():
    _,events,_=reviewed_legacy_events(legacy_position_archive(),date(2011,1,3))
    assert event_at(events,'2011-04-08T08:45:00+08:00')['after'] == 5000
    assert event_at(events,'2011-04-11T08:45:00+08:00')['after'] == 6000
    with pytest.raises(ValueError,match='tightening'):
        reviewed_legacy_events(legacy_position_archive(after=4000),date(2011,1,3))
    with pytest.raises(ValueError,match='different source bytes'):
        reviewed_legacy_events(legacy_position_archive(digest='b'*64),date(2011,1,3))
    with pytest.raises(ValueError,match='predates'):
        reviewed_legacy_events(legacy_position_archive(),date(2010,1,3))


@pytest.mark.parametrize('clause,expected', [
    ('自然人與法人部位限制數分別由12,000口與24,000口調降至10,000口與20,000口，「臺指選擇權」自然人與法人部位限制數分別由35,000口與80,000口調降至30,000口與70,000口，自111年3月17日一般交易時段起生效。', (12000,10000,'2022-03-17')),
    ('自然人部位限制數由10,000個契約調降至9,000個契約、法人部位限制數由20,000個契約調降至18,000個契約，自114年12月18日一般交易時段起生效。「臺指選擇權」法人部位限制數由60,000個契約調升至70,000個契約，自114年10月16日一般交易時段起生效。', (10000,9000,'2025-12-18')),
    ('法人部位限制數由22,000口調降至20,000口，自114年9月18日一般交易時段起生效。', None),
])
def test_position_rule_selects_natural_person_and_tx_clock(clause, expected):
    result = position_change('二、「臺股期貨」' + clause + '三、其他股票期貨')
    assert (result['before'], result['after'], result['effective_at'][:10]) == expected if expected else result is None


def test_event_chain_and_two_session_phases_are_causal():
    first = dict(before=[167000,128000], after=[184000,141000], known_at='2021-05-18T23:59:59+08:00', effective_at='2021-05-19T13:45:00+08:00')
    second = dict(before=first['after'], after=[203000,156000], known_at='2022-01-25T23:59:59+08:00', effective_at='2022-01-26T13:45:00+08:00')
    events = validate_event_chain([second, first], ('before','after'))
    assert event_at(events, '2022-01-26T08:45:00+08:00') == first
    assert event_at(events, '2022-01-26T13:45:00+08:00') == second
    with pytest.raises(ValueError, match='transition'):
        validate_event_chain([first, dict(second, before=[1,1])], ('before','after'))
    with pytest.raises(ValueError, match='public'):
        validate_event_chain([dict(first, known_at='2021-05-20T23:59:59+08:00')], ('before','after'))


def test_candidate_press_body_clock_and_explicit_listing_date():
    from scripts.build_tw_futures_margin_event_candidates import candidate_notice_clock
    press='臺灣期貨交易所新聞稿 部門：結算部 中華民國110年10月13日 臺灣期貨交易所於110年10月13日公告調整，並自110年10月14日一般交易時段結束後起實施。'
    clock=candidate_notice_clock(press,'2021-10-13')
    assert clock['issue_date_bound'] and clock['chronological']
    assert clock['effective_date']=='2021-10-14'
    assert not candidate_notice_clock(press,'2020-10-13')['issue_date_bound']
    assert not candidate_notice_clock('發文日期：中華民國113年10月13日 '+press,'2021-10-13')['issue_date_bound']
    disposal=('臺灣期貨交易所新聞稿 部門：結算部 中華民國109年5月19日 '
              '臺灣期貨交易所於109年5月20日調高，期交所依規定調高保證金，'
              '自109年5月20日(證券市場處置生效日次一營業日)該契約交易時段結束後起實施。')
    result=candidate_notice_clock(disposal,'2020-05-19')
    assert result['issue_date_bound'] and result['effective_date']=='2020-05-20'
    assert result['effective_phase']=='after_product_trading_session_unspecified'
    listing='發文日期：中華民國113年10月28日 前揭契約訂於113年11月1日上市。'
    clock=candidate_notice_clock(listing,'2024-10-28')
    assert clock['effective_phase']=='new_contract_listing' and clock['effective_date']=='2024-11-01'
    assert candidate_notice_clock(listing+'生效日期：113年11月1日','2024-10-28')['effective_phase']=='new_contract_listing'
    # Multiple product dates cannot silently become a single launch clock.
    assert candidate_notice_clock(listing+'另一契約訂於113年11月4日上市。','2024-10-28')['effective_date'] is None
    listing='發文日期：中華民國111年1月24日 旨揭契約上市日期為111年2月7日，交割月份為111年2月。'
    clock=candidate_notice_clock(listing,'2022-01-24')
    assert clock['effective_phase']=='new_contract_listing' and clock['effective_date']=='2022-02-07'


def test_partial_native_tables_do_not_hide_reviewed_scanned_notice_clock(tmp_path):
    from scripts.build_tw_futures_margin_event_candidates import refresh_candidate_clocks
    text='發文日期：中華民國110年10月13日 並自110年10月14日一般交易時段結束後起實施。'
    (tmp_path/'review.txt').write_text(text)
    (tmp_path/'tables.json').write_text(json.dumps({'pages':[{'native_text':'原始 維持 結算 保證金金額'}]}))
    receipt={'status':'complete','content_sha256':'original'}
    (tmp_path/'native_receipt.json').write_text(json.dumps(receipt))
    (tmp_path/'review_receipt.json').write_text(json.dumps(dict(receipt,
        files=[dict(path='candidate.txt',sha256=sha256_file(tmp_path/'review.txt'))])))
    kinds={'review.txt':'review_pages_including_ocr','tables.json':'native_table_tables.json',
           'native_receipt.json':'native_table_receipt.json','review_receipt.json':'page_extraction_receipt'}
    archive=SimpleNamespace(bundle=tmp_path,document=lambda url:{'content_sha256':'original','text':''},
        sources={p:dict(path=p,url='official',kind=k,sha256=sha256_file(tmp_path/p)) for p,k in kinds.items()})
    row=dict(source_url='official',source_content_sha256='original',published_date='2021-10-13',
             extraction='canonical_native',issue_date_bound=False,point_in_time_verified=False)
    assert refresh_candidate_clocks(archive,[row],[])==1
    assert row['issue_date_bound'] and row['effective_date']=='2021-10-14'
    assert row['point_in_time_verified'] is False
    (tmp_path/'native_receipt.json').write_text(json.dumps(dict(receipt,content_sha256='different')))
    with pytest.raises(ValueError,match='different source'):
        refresh_candidate_clocks(archive,[row],[])


def daily_fixture(tmp_path):
    source = tmp_path/'source'; source.mkdir()
    proof = tmp_path/'proof'; proof.mkdir()
    final = tmp_path/'final'; final.mkdir()
    raw = source/'raw.csv'; raw.write_text('original raw evidence')
    rows=[]
    for physical, days, settlement in [('TX:202106',[18,19,20],100.),('TX:202107',[20,21],200.)]:
        for day in days:
            rows.append(dict(date=date(2021,5,day), product='TX', contract=physical.split(':')[1],
                physical_contract=physical, symbol='TAIFEX_SLOT_'+('0001' if days[0]==18 else '0002'),
                settlement=settlement, volume=10., open_interest=20., source_row_observed=True,
                open=settlement, close=settlement, can_hold_overnight=day != days[-1],
                liquidation_reason='carry_same_contract', executable=True,
                must_liquidate=False, resolved_last_trade_date=date(2021,6,16),
                previous_market_date=date(2021,5,day-1)))
    data=source/'continuous_daily.parquet'; pl.DataFrame(rows).write_parquet(data)
    (source/'manifest.json').write_text(json.dumps({'outputs':{'continuous_daily':{'sha256':sha256_file(data)}}}))
    p=proof/'official_evidence.parquet'
    pl.DataFrame([dict(date=r['date'],physical_contract=r['physical_contract'],
                      official_settlement=str(r['settlement']),official_source_sha256=sha256_file(raw)) for r in rows]).write_parquet(p)
    (proof/'official_evidence_manifest.json').write_text(json.dumps({'source':'taifex_complete_daily_and_spread_legs_v1',
        'sha256':sha256_file(p),'sources':[{'path':str(raw),'sha256':sha256_file(raw)}]}))
    f=final/'final.parquet'
    pl.DataFrame(schema={'settlement_date':pl.Date,'product':pl.String,'contract':pl.String,'final_settlement_price':pl.Float64}).write_parquet(f)
    (final/'manifest.json').write_text(json.dumps({'outputs':{'futures_final_settlement_history':{'sha256':sha256_file(f)}}}))
    return data,p,f,raw


def test_daily_scope_uses_physical_prior_and_excludes_only_first_observation(tmp_path):
    daily, proof, final, _ = daily_fixture(tmp_path)
    # Inject the exact source error: a recycled slot's previous price is another asset.
    f=pl.read_parquet(daily).with_columns(pl.lit(9000.).alias('previous_settlement'))
    f.write_parquet(daily)
    daily.with_name('manifest.json').write_text(json.dumps({'outputs':{'continuous_daily':{'sha256':sha256_file(daily)}}}))
    result, summary = prepare_daily(daily,proof,final,tmp_path/'out',date(2021,5,20),date(2021,5,21))
    new = result.filter(pl.col('physical_contract')=='TX:202107')
    assert new['date'].to_list() == [date(2021,5,21)]
    assert new['previous_settlement'].to_list() == [200.]
    assert summary['excluded_first_observations'] == 1


def test_daily_rejects_tampered_raw_source(tmp_path):
    daily, proof, final, raw = daily_fixture(tmp_path)
    raw.write_text('tampered')
    with pytest.raises(ValueError, match='raw archive SHA'):
        prepare_daily(daily,proof,final,tmp_path/'out',date(2021,5,20),date(2021,5,21))


def test_visual_rule_review_binds_amendment_clock_and_per_row_limits(tmp_path):
    from scripts.build_tw_futures_margin_event_candidates import source_review_candidates
    c=sqlite3.connect(':memory:');c.row_factory=sqlite3.Row
    c.executescript("CREATE TABLE announcements(url,published_date);"
                   "INSERT INTO announcements VALUES('launch','2013-11-11'),('amendment','2013-11-19');")
    archive=SimpleNamespace(conn=c,document=lambda url: {'content_sha256':url},copy=lambda *a,**kw:None)
    review=dict(review_kind='source_bound_visual_transcription',source_url='launch',content_sha256='launch',
        published_date='2013-11-11',known_at='2013-11-19T23:59:59+08:00',effective_date='2013-11-21',
        effective_phase='product_regular_open',margin_kind='notional_rate',
        amendments=[dict(source_url='amendment',content_sha256='amendment',published_date='2013-11-19')],
        rows=[dict(product='MFF',after=dict(initial=.135,maintenance=.1035,clearing=.1),natural_person_limit=2000),
              dict(product='DDF',after=dict(initial=.162,maintenance=.1242,clearing=.12),natural_person_limit=8000)])
    path=tmp_path/'review.json'
    path.write_text(json.dumps({'reviews':[review]}))
    margins,positions=source_review_candidates(archive,path)
    assert [m['after'] for m in margins]==[[.135,.1035,.1],[.162,.1242,.12]]
    assert [p['natural_person_limit'] for p in positions]==[2000,8000]
    assert all(not f['point_in_time_verified'] for f in margins+positions)
    review['rows'][0]['before'] = dict(initial=.162, maintenance=.1242, clearing=.12)
    path.write_text(json.dumps(review))
    margins, _ = source_review_candidates(archive, path)
    assert margins[0]['before'] == [.162, .1242, .12]
    assert margins[0]['event_type'] == 'before_after'
    assert margins[1]['before'] is None
    review['rows'][0]['before']['maintenance'] = .2
    path.write_text(json.dumps(review))
    with pytest.raises(ValueError, match='prior margin hierarchy'):
        source_review_candidates(archive, path)
    review['rows'][0].pop('before')
    review['known_at']='2013-11-11T23:59:59+08:00'
    path.write_text(json.dumps({'reviews':[review]}))
    with pytest.raises(ValueError,match='clock'):
        source_review_candidates(archive,path)
    review['known_at']='2013-11-19T23:59:59+08:00'
    review['amendments'][0]['content_sha256']='wrong'
    path.write_text(json.dumps(review))
    with pytest.raises(ValueError,match='SHA'):
        source_review_candidates(archive,path)


def test_candidate_reuse_preserves_provenance_and_rejects_tampering(tmp_path):
    from scripts.build_tw_futures_margin_event_candidates import reuse_candidate_bundle
    parent=tmp_path/'parent';parent.mkdir()
    for name in ('margin','position','corporate'):
        pl.DataFrame({'product':['TX'],'point_in_time_verified':[False]}).write_parquet(
            parent/(name+'_event_candidates.parquet'))
    (parent/'announcement_audit.json').write_text('[{"announcement_url":"official"}]')
    (parent/'product_event_coverage.csv').write_text('product\nTX\n')
    source=parent/'source.txt';source.write_text('retained original')
    outputs={p.name:{'sha256':sha256_file(p)} for p in parent.iterdir() if p!=source}
    receipt=dict(schema_version=1,point_in_time_verified=False,all_products_training_ready=False,
                 outputs=outputs,sources=[dict(path='source.txt',sha256=sha256_file(source),url='official',kind='raw')])
    (parent/'manifest.json').write_text(json.dumps(receipt))
    copied=[]
    def copy(path,digest,**proof):
        if sha256_file(path)!=digest:
            raise ValueError('source SHA mismatch')
        copied.append((path,proof))
    archive=SimpleNamespace(copy=copy)
    *groups,audit=reuse_candidate_bundle(archive,parent)
    assert all(g[0]['product']=='TX' for g in groups)
    assert audit[0]['announcement_url']=='official'
    assert copied[-1][1]['kind']=='parent_candidate_manifest'
    source.write_text('changed')
    with pytest.raises(ValueError,match='SHA'):
        reuse_candidate_bundle(archive,parent)
    source.write_text('retained original')
    (parent/'product_event_coverage.csv').write_text('changed')
    with pytest.raises(ValueError,match='output SHA'):
        reuse_candidate_bundle(archive,parent)


@pytest.mark.parametrize('expiry', [False, True])
def test_snapshot_end_marks_open_account_without_fake_liquidation(tmp_path, expiry):
    daily, proof, final, _ = daily_fixture(tmp_path)
    f = pl.read_parquet(daily).with_columns(
        pl.when(pl.col('date') == date(2021,5,21)).then(pl.lit('dataset_terminal'))
        .otherwise(pl.col('liquidation_reason')).alias('liquidation_reason'),
        (pl.col('date') == date(2021,5,21)).alias('must_liquidate'),
        pl.lit(date(2021,5,21) if expiry else date(2021,6,16)).alias('resolved_last_trade_date'))
    if expiry:
        pl.DataFrame({'settlement_date':[date(2021,5,21)],'product':['TX'],'contract':['202107'],
                      'final_settlement_price':[205.]}).write_parquet(final)
        final.with_name('manifest.json').write_text(json.dumps({'outputs':{
            'futures_final_settlement_history':{'sha256':sha256_file(final)}}}))
    f.write_parquet(daily)
    daily.with_name('manifest.json').write_text(json.dumps({'outputs':{'continuous_daily':{'sha256':sha256_file(daily)}}}))
    result, summary = prepare_daily(daily,proof,final,tmp_path/'out',date(2021,5,20),date(2021,5,21))
    end = result.filter(pl.col('date') == date(2021,5,21))
    assert end['liquidation_reason'].to_list() == ['last_trade_date' if expiry else 'dataset_boundary_mark_only']
    assert end['must_liquidate'].any() == expiry
    assert end['settlement'].to_list() == [205. if expiry else 200.]
    assert summary['snapshot_boundary_mark_only_rows'] == (0 if expiry else 1)
