"""Halt marks require their own month, source, generation and prior session."""
from datetime import date
from copy import deepcopy
import polars as pl
import pytest
from stockagent.data.tw_futures_margin_preparation import apply_loss_reduction_halt_values,load_loss_reduction_halt_review


def halt_case():
    rows=[]
    for month,price in [('201509',12.33),('201512',11.78)]:
        for day in (2,3,4,7):
            prior=day==2
            rows.append(dict(date=date(2015,9,day),product='AAF',contract=month,
                physical_instance=f'AAF:{month}#old',daily_mark=price if prior else None,
                valuation_price=price if prior else None,cash_settlement=day==4 and month=='201509',
                official_source_sha256='a'*64 if prior else None,
                official_settlement=str(price) if prior else None,
                outright_volume=None,official_open=None,official_close=None))
    frame=pl.DataFrame(rows,schema_overrides={'official_open':pl.String,'official_close':pl.String,
        'outright_volume':pl.Int64})
    calendar=frame.select('date').unique()
    episode=dict(product='AAF',contract_months=['201509','201512'],halt_start_date='2015-09-03',
        halt_end_date='2015-09-04',resumption_date='2015-09-07',ratio='0.8',
        before_units='2000',after_units='1600',rule_known_at='2015-08-26T23:59:59+08:00',
        law_effective_date=date(2011,5,3),law_source_sha256='b'*64,
        law_regime_source_sha256='b'*64,law_valid_until_date_exclusive=None,
        legal_halt_value_contract='source_bound_loss_reduction_halt_value_v2',
        source_content_sha256='c'*64,review_sha256='d'*64)
    return frame,calendar,episode


def test_own_month_legal_values_preserve_missing_quotes_and_terminal_marks():
    frame,calendar,e=halt_case();original=frame.clone()
    result,values=apply_loss_reduction_halt_values(frame,calendar,e)
    assert frame.equals(original) and values.height==3
    assert result.drop(*values.columns[4:]).drop('valuation_price').equals(frame.drop('valuation_price'))
    assert values.filter(pl.col('contract')=='201512')['legal_halt_valuation_price'].to_list()==[11.78,11.78]
    assert values.filter(pl.col('contract')=='201509')['legal_halt_contract_value_twd'].to_list()==[24660.]
    assert result.filter(pl.col('cash_settlement'))['valuation_price'].null_count()==1
    assert result.filter(pl.col('date')==date(2015,9,7))['valuation_price'].null_count()==2
    assert values['legal_halt_prior_date'].to_list()==[date(2015,9,2)]*3
    assert values['legal_halt_known_at'].to_list()==['2015-09-02T23:59:59+08:00']*3
    assert values['legal_halt_rule_known_at'].to_list()==[e['rule_known_at']]*3


@pytest.mark.parametrize('problem',['missing_month','wrong_generation','stale_day','no_source',
    'bad_source','no_price','invalid_price','literal_mismatch'])
def test_missing_own_prior_evidence_never_borrows_or_searches_a_later_mark(problem):
    frame,calendar,e=halt_case();prior=(pl.col('date')==date(2015,9,2))&(pl.col('contract')=='201512')
    if problem=='missing_month':frame=frame.filter(~prior)
    elif problem=='wrong_generation':frame=frame.with_columns(pl.when(prior).then(pl.lit('new_generation'))
        .otherwise(pl.col('physical_instance')).alias('physical_instance'))
    elif problem=='stale_day':frame=frame.with_columns(pl.when(prior).then(pl.lit(date(2015,9,1)))
        .otherwise(pl.col('date')).alias('date'))
    else:
        field={'no_source':'official_source_sha256','bad_source':'official_source_sha256',
            'no_price':'daily_mark','invalid_price':'daily_mark','literal_mismatch':'official_settlement'}[problem]
        value={'no_source':None,'bad_source':'x','no_price':None,'invalid_price':-1.,'literal_mismatch':'99'}[problem]
        frame=frame.with_columns(pl.when(prior).then(pl.lit(value)).otherwise(pl.col(field)).alias(field))
    result,values=apply_loss_reduction_halt_values(frame,calendar,e)
    assert not values.filter(pl.col('contract')=='201512').height
    assert result.filter((pl.col('contract')=='201512')&pl.col('date').is_between(date(2015,9,3),date(2015,9,4)))['valuation_price'].null_count()==2


@pytest.mark.parametrize('problem',['ratio','late_clock','naive_clock','future_rule','end','source',
    'before_units','after_units','duplicate_day','duplicate_identity','executed_volume','executed_price'])
def test_invalid_or_contradictory_halt_contracts_are_rejected(problem):
    frame,calendar,e=halt_case();e=deepcopy(e)
    if problem=='ratio':e['ratio']='1'
    elif problem=='late_clock':e['rule_known_at']='2015-09-03T23:59:59+08:00'
    elif problem=='naive_clock':e['rule_known_at']='2015-08-26T23:59:59'
    elif problem=='future_rule':e['law_effective_date']=date(2015,9,4)
    elif problem=='end':e['halt_end_date']='2015-09-07'
    elif problem=='source':e['law_source_sha256']='x'
    elif problem=='before_units':e['before_units']='2100'
    elif problem=='after_units':e['after_units']='1600.01'
    elif problem=='duplicate_day':calendar=pl.concat([calendar,calendar.head(1)])
    elif problem=='duplicate_identity':frame=pl.concat([frame,frame.head(1)])
    else:
        field='outright_volume' if problem=='executed_volume' else 'official_open'
        value=1 if problem=='executed_volume' else '12.34'
        frame=frame.with_columns(pl.when(pl.col('date')==date(2015,9,3)).then(pl.lit(value))
            .otherwise(pl.col(field)).alias(field))
    with pytest.raises(ValueError):apply_loss_reduction_halt_values(frame,calendar,e)


def test_existing_official_daily_and_final_marks_have_priority():
    frame,calendar,e=halt_case()
    frame=frame.with_columns(pl.when(pl.col('date')==date(2015,9,4)).then(pl.lit(12.35))
        .otherwise(pl.col('valuation_price')).alias('valuation_price'))
    result,values=apply_loss_reduction_halt_values(frame,calendar,e)
    assert values.height==2
    assert result.filter(pl.col('date')==date(2015,9,4))['valuation_price'].to_list()==[12.35,12.35]


def test_disjoint_episodes_preserve_prior_provenance_and_reapplication_is_idempotent():
    frame,calendar,e=halt_case()
    second=frame.with_columns(pl.col('date').dt.offset_by('1y'),
        pl.col('contract').str.replace('2015','2016'),
        pl.col('physical_instance').str.replace('2015','2016'))
    both=pl.concat([frame,second]);dates=both.select('date').unique()
    later=deepcopy(e)
    later.update(contract_months=['201609','201612'],halt_start_date='2016-09-03',
        halt_end_date='2016-09-04',resumption_date='2016-09-07',
        rule_known_at='2016-08-26T23:59:59+08:00',review_sha256='e'*64,
        source_content_sha256='f'*64,ratio='0.7',after_units='1400')
    first,first_values=apply_loss_reduction_halt_values(both,dates,e)
    result,later_values=apply_loss_reduction_halt_values(first,dates,later)
    assert first_values.height==later_values.height==3
    assert result.filter(pl.col('date').dt.year()==2015).equals(
        first.filter(pl.col('date').dt.year()==2015))
    assert result.filter(pl.col('legal_halt_review_sha256')=='e'*64).height==3
    assert result.columns==first.columns
    repeated,values=apply_loss_reduction_halt_values(result,dates,e)
    assert repeated.equals(result) and values.is_empty()


@pytest.mark.parametrize('problem',['overlap','incomplete'])
def test_legal_halt_provenance_cannot_be_reassigned(problem):
    frame,calendar,e=halt_case()
    marked,_=apply_loss_reduction_halt_values(frame,calendar,e)
    if problem=='overlap':e=dict(e,review_sha256='e'*64)
    else:marked=marked.drop('legal_halt_prior_source_sha256')
    with pytest.raises(ValueError):apply_loss_reduction_halt_values(marked,calendar,e)


def source_review_case(tmp_path,*,edition='2011',event_year=2015,wording='減資以彌補虧損'):
    import gzip,json,hashlib
    from downloader.artifact_io import atomic_write_json,sha256_file
    formula='四、消除累積虧損之減資。期貨契約價值(平常日遇停止交易)＝契約停止交易前一營業日之期貨價格(每日結算價)÷減資換發新股比例×減資後約定標的證券股數'
    amendment='發文日期：中華民國100年2月1日。發文字號：台期交字第10002003020號。公告股票期貨契約遇契約調整契約價值及約定標的物價值計算方式如附件2，本公司99年1月14日台期交字第09900006290號公告自100年5月3日起不再適用。'+formula
    law=amendment if edition=='2011' else '發文日期：中華民國99年1月14日。發文字號：台期交字第09900006290號。'+formula
    body=f'發文日期：中華民國104年8月26日。{wording}，每千股換發新股票800股。停止交易期間：104年9月3日至4日。恢復交易日：104年9月7日。104年9月契約(AAF)到期日為104年9月4日。'
    sources=[]
    def source(url,text,kind='parsed_native'):
        blob=('%PDF fixture '+text).encode();digest=hashlib.sha256(blob).hexdigest()
        raw=tmp_path/(digest+'.gz');raw.write_bytes(gzip.compress(blob))
        sources.append(dict(path=raw.name,sha256=sha256_file(raw),url=url,kind='raw_gzip'))
        if kind:
            parsed=tmp_path/(digest+'.json');atomic_write_json(parsed,dict(text=text,content_sha256=digest))
            sources.append(dict(path=parsed.name,sha256=sha256_file(parsed),url=url,kind=kind))
        return digest
    if event_year!=2015:body=body.replace('104年',f'{event_year-1911}年')
    law_hash=source('law',law);source('amendment',amendment)
    event_hash=source('event',body,kind=None)
    text=tmp_path/'candidate.txt';text.write_text(body)
    sources.append(dict(path=text.name,sha256=sha256_file(text),url='event',kind='review_pages_including_ocr'))
    receipt=tmp_path/'pages.json';atomic_write_json(receipt,dict(status='complete',content_sha256=event_hash,
        document_pages=2,extracted_pages=2,files=[dict(path='candidate.txt',sha256=sha256_file(text))]))
    sources.append(dict(path=receipt.name,sha256=sha256_file(receipt),url='event',kind='page_extraction_receipt'))
    commencement_sources=[]
    for url,content in [('fixing','股票期貨契約之最後結算價,以最後結算日證券市場當日交易時間收盤前六十分鐘內標的證券之算術平均價訂之。未了結之部位於最後結算日依最後結算價計算約定標的物價值'),
                        ('start','發文日期：中華民國98年12月4日。第09800110800號公告，自99年1月25日起實施')]:
        before=len(sources);source(url,content,kind='parsed_v2');commencement_sources.extend(sources[before:])
    commencement=tmp_path/'commencement.json';atomic_write_json(commencement,dict(
        review_kind='source_bound_same_security_final_fixing',sources=commencement_sources,
        law_source_url='fixing',commencement_source_url='start',effective_date='2010-01-25',known_at='2009-12-04T23:59:59+08:00'))
    cf=tmp_path/'corporate.parquet';pl.DataFrame([dict(source_content_sha256=event_hash,from_product='AAF',
        effective_date=f'{event_year}-09-07',contract_multiplier=1600.,deliverable_cash_twd=0.,requires_rights_valuation=False,
        deliverable_components_resolved=True,known_at=f'{event_year}-08-26T23:59:59+08:00',contract_months=[f'{event_year}12'])]).write_parquet(cf)
    atomic_write_json(tmp_path/'manifest.json',dict(outputs={cf.name:dict(sha256=sha256_file(cf))}))
    review=dict(review_kind='source_bound_loss_reduction_halt_value_v2',sources=sources,
        law_source_url='law',law_known_at=('2011-02-01' if edition=='2011' else '2010-01-14')+'T23:59:59+08:00',
        law_regime_source_url='amendment',law_regime_known_at='2011-02-01T23:59:59+08:00',
        commencement_review=dict(path=commencement.name,sha256=sha256_file(commencement)),
        corporate_input=dict(path=cf.name,sha256=sha256_file(cf)),episode=dict(product='AAF',source_url='event',
            source_content_sha256=event_hash,halt_start_date=f'{event_year}-09-03',halt_end_date=f'{event_year}-09-04',
            resumption_date=f'{event_year}-09-07',before_units='2000',after_units='1600',new_shares_per_thousand='800',
            known_at=f'{event_year}-08-26T23:59:59+08:00',expiring_contracts=[dict(contract=f'{event_year}09',final_date=f'{event_year}-09-04')]))
    path=tmp_path/'review.json';atomic_write_json(path,review)
    return path,review


@pytest.mark.parametrize('wording', ['減資以彌補虧損', '減資彌補虧損', '現金減資退還股款', '減資'])
def test_source_owned_loss_wording_does_not_admit_cash_or_generic_reductions(tmp_path, wording):
    path, _ = source_review_case(tmp_path, wording=wording)
    if wording in ('減資以彌補虧損', '減資彌補虧損'):
        assert load_loss_reduction_halt_review(path)['ratio'] == '0.8'
    else:
        with pytest.raises(ValueError, match='not pure loss reduction'):
            load_loss_reduction_halt_review(path)


def test_portable_review_reconciles_originals_commencement_and_owned_months(tmp_path):
    path,_=source_review_case(tmp_path);e=load_loss_reduction_halt_review(path)
    assert e['contract_months']==['201509','201512'] and e['ratio']=='0.8'
    assert e['law_effective_date']==date(2011,5,3) and e['law_valid_until_date_exclusive'] is None


def test_original_2010_formula_is_admitted_only_before_its_explicit_replacement(tmp_path):
    path,_=source_review_case(tmp_path,edition='2010',event_year=2010)
    e=load_loss_reduction_halt_review(path)
    assert e['law_effective_date']==date(2010,1,25)
    assert e['law_valid_until_date_exclusive']==date(2011,5,3)


@pytest.mark.parametrize('edition,event_year',[('2010',2015),('2011',2010)])
def test_same_formula_cannot_cross_its_actual_law_regime(tmp_path,edition,event_year):
    path,_=source_review_case(tmp_path,edition=edition,event_year=event_year)
    with pytest.raises(ValueError,match='outside its formula law effective regime'):
        load_loss_reduction_halt_review(path)


def test_old_unversioned_review_is_not_silently_upgraded(tmp_path):
    from downloader.artifact_io import atomic_write_json
    path,r=source_review_case(tmp_path);r['review_kind']='source_bound_loss_reduction_halt_value_v1'
    atomic_write_json(path,r)
    with pytest.raises(ValueError,match='unsupported loss-reduction halt review'):
        load_loss_reduction_halt_review(path)


def test_direct_halt_overlay_rejects_a_regime_ending_inside_the_episode():
    f,c,e=halt_case();e['law_valid_until_date_exclusive']=date(2015,9,4)
    with pytest.raises(ValueError,match='invalid/unavailable'):
        apply_loss_reduction_halt_values(f,c,e)


@pytest.mark.parametrize('problem',['edited_source','partial_pages','unbound_text','cash_return','wrong_clock',
    'wrong_dates','wrong_ratio','wrong_quantity','wrong_generation','wrong_expiry','corporate_cash','corporate_rights'])
def test_review_rejects_unproved_source_clock_identity_and_financial_components(tmp_path,problem):
    import json
    from downloader.artifact_io import atomic_write_json,sha256_file
    path,r=source_review_case(tmp_path)
    if problem in ('edited_source','cash_return'):
        f=tmp_path/'candidate.txt';f.write_text(f.read_text().replace('減資以彌補虧損','現金減資退還股款'))
        if problem=='cash_return':
            s=next(s for s in r['sources'] if s['kind']=='review_pages_including_ocr');s['sha256']=sha256_file(f)
            receipt=tmp_path/'pages.json';v=json.loads(receipt.read_text());v['files'][0]['sha256']=sha256_file(f);atomic_write_json(receipt,v)
            next(s for s in r['sources'] if s['kind']=='page_extraction_receipt')['sha256']=sha256_file(receipt)
    elif problem in ('partial_pages','unbound_text'):
        f=tmp_path/'pages.json';v=json.loads(f.read_text())
        if problem=='partial_pages':v['extracted_pages']=1
        else:v['files'][0]['sha256']='e'*64
        atomic_write_json(f,v);next(s for s in r['sources'] if s['kind']=='page_extraction_receipt')['sha256']=sha256_file(f)
    elif problem.startswith('corporate_'):
        f=tmp_path/'corporate.parquet';d=pl.read_parquet(f)
        column='deliverable_cash_twd' if problem=='corporate_cash' else 'requires_rights_valuation'
        d.with_columns(pl.lit(1. if problem=='corporate_cash' else True).alias(column)).write_parquet(f)
        r['corporate_input']['sha256']=sha256_file(f);atomic_write_json(tmp_path/'manifest.json',dict(outputs={f.name:dict(sha256=sha256_file(f))}))
    else:
        if problem=='wrong_clock':r['episode']['known_at']='2015-09-03T23:59:59+08:00'
        elif problem=='wrong_dates':r['episode']['halt_start_date']='2015-09-02'
        elif problem=='wrong_ratio':r['episode']['new_shares_per_thousand']='801'
        elif problem=='wrong_quantity':r['episode']['after_units']='1600.1'
        elif problem=='wrong_generation':r['episode']['product']='AA1'
        elif problem=='wrong_expiry':r['episode']['expiring_contracts'][0]['contract']='201510'
    atomic_write_json(path,r)
    with pytest.raises(ValueError):load_loss_reduction_halt_review(path)
