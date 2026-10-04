from datetime import date
import json

import polars as pl
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_finmind_ticks import (
    SOURCE, SCHEMA_VERSION, normalize_finmind_futures_ticks,
    read_finmind_contract_day, source_paths,
)
from scripts.download_tw_futures_intraday_gaps import candidate_plan, valid_saved

DAY = date(2020, 3, 23)


@pytest.mark.parametrize('reason', ['deadline_reached','stopped_by_file'])
def test_bounded_download_rechecks_stop_after_shared_limiter_wait(tmp_path,monkeypatch,reason):
    from datetime import datetime,UTC,timedelta
    import requests
    from scripts.download_tw_futures_intraday_gaps import BoundedSession,AcquisitionStopped
    from downloader.download_finmind_complement import _fetch_rows
    stop=tmp_path/'STOP'
    class DelayedLimiter:
        def wait(self):
            if reason=='stopped_by_file':stop.touch()
    def forbidden_http(*args,**kwargs):
        raise AssertionError('HTTP must not begin after the bounded acquisition stops')
    monkeypatch.setattr(requests.Session,'get',forbidden_http)
    deadline=datetime.now(UTC)-timedelta(seconds=1) if reason=='deadline_reached' else None
    with BoundedSession(deadline,stop) as session:
        with pytest.raises(AcquisitionStopped,match=reason):
            _fetch_rows(session,DelayedLimiter(),tmp_path,'TaiwanFuturesTick','test-token',
                        {'dataset':'TaiwanFuturesTick'},max_response_bytes=1024)


def ticks():
    return pl.DataFrame({
        'date':['2020-03-23 09:00:01']*2+['2020-03-23 09:00:20']*3,
        'futures_id':['DDF']*5,
        'contract_date':['202004','202004','202004','202005','202004/202005'],
        'price':[10.,10.,11.,99.,-1.], 'volume':[2,2,4,100,100],
    })


def normalize(frame):
    return normalize_finmind_futures_ticks(frame,day=DAY,product='DDF',
        physical_contract='DDF:202004',source_sha256='a'*64,asset_class='stock_future')


def saved(tmp_path):
    data, receipt = source_paths(tmp_path,'DDF',DAY)
    atomic_write_parquet(data,ticks())
    atomic_write_json(receipt,dict(source=SOURCE,schema_version=SCHEMA_VERSION,
        product='DDF',date=str(DAY),status='complete',rows=5,sha256=sha256_file(data)))
    row=dict(date=DAY,product='DDF',physical_contract='DDF:202004',asset_class='stock_future',
             volume=4,source_row_observed=True)
    fact=dict(date=DAY,physical_contract='DDF:202004',official_high='11',official_low='10',
              outright_volume=4,official_reason='outright_volume')
    return data,receipt,row,fact


def test_exact_month_excludes_spreads_preserves_print_duplicates_and_right_clock():
    bars,stats=normalize(ticks())
    assert stats['tick_rows']==3
    assert stats['tick_volume']==4
    assert bars['minute'].to_list()==[541]
    assert bars['volume'].to_list()==[4.]
    assert bars['vwap'].to_list()==[10.5]
    assert bars['close'].to_list()==[11.]


def test_unrelated_delivery_month_null_price_does_not_invalidate_target_prints():
    frame=ticks().with_columns(pl.when(pl.col('contract_date')=='202005').then(None).otherwise(pl.col('price')).alias('price'))
    assert normalize(frame)[1]['tick_volume']==4


def test_official_week_contract_identity_is_matched_without_inventing_expiry():
    frame=pl.DataFrame({'date':['2020-11-26 09:00:01']*3,'futures_id':['MTX']*3,
        'contract_date':['202012W1','202012','202012W1/202012'],
        'price':[13701.,14000.,-1.],'volume':[2,20,100]})
    bars,stats=normalize_finmind_futures_ticks(frame,day=date(2020,11,26),product='MTX',
        physical_contract='MTX:202012W1',source_sha256='a'*64,asset_class='index_future')
    assert stats['tick_volume']==1
    assert bars['physical_contract'].to_list()==['MTX:202012W1']
    assert bars['vwap'].to_list()==[13701.]


@pytest.mark.parametrize('column,value,message',[
    ('futures_id','OTHER','outside'),('date','2020-03-24 09:00:01','outside'),
    ('volume',3,'even'),('volume',0,'positive'),
])
def test_wrong_query_identity_or_quantity_cannot_enter_tape(column,value,message):
    with pytest.raises(ValueError,match=message):
        normalize(ticks().with_columns(pl.lit(value).alias(column)))


def test_foreign_etf_extended_session_keeps_1612_observation():
    frame=pl.DataFrame({'date':['2020-03-23 16:12:00'],'futures_id':['OCF'],
        'contract_date':['202004'],'price':[20.15],'volume':[4]})
    bars,stats=normalize_finmind_futures_ticks(frame,day=DAY,product='OCF',
        physical_contract='OCF:202004',source_sha256='a'*64,asset_class='etf_future')
    assert bars['minute'].to_list()==[973]
    assert stats['tick_volume']==2


def test_reader_requires_receipt_hash_and_official_bounds(tmp_path):
    data,receipt,row,fact=saved(tmp_path)
    bars,evidence=read_finmind_contract_day(tmp_path,row,fact)
    assert evidence['status']=='minute_verified'
    assert evidence['repair_kind']=='finmind_ticks'
    assert bars['volume'].sum()==4
    assert valid_saved(data,receipt,product='DDF',day=DAY)
    altered=json.loads(receipt.read_text());altered['sha256']='0'*64
    atomic_write_json(receipt,altered)
    assert read_finmind_contract_day(tmp_path,row,fact)[1]['status']=='invalid_source'
    assert not valid_saved(data,receipt,product='DDF',day=DAY)


@pytest.mark.parametrize('mutation', ['missing','wrong_identity','volume','price'])
def test_missing_or_conflicting_official_evidence_fails_closed(tmp_path,mutation):
    _,_,row,fact=saved(tmp_path)
    if mutation=='missing':fact=None
    elif mutation=='wrong_identity':fact['physical_contract']='DDF:202005'
    elif mutation=='volume':fact['outright_volume']=3
    else:fact['official_high']='10'
    bars,evidence=read_finmind_contract_day(tmp_path,row,fact)
    assert bars.is_empty()
    assert evidence['status']=='invalid_source'


def test_gap_plan_omits_only_existing_raw_or_proven_zero_integer_capacity(tmp_path):
    file=tmp_path/'gaps.csv'
    pl.DataFrame({'date':[DAY]*4,'physical_contract':['DDF:202004','CDF:202004','FCF:202004','ABC:202004'],
        'product':['DDF','CDF','FCF','ABC'],'asset_class':['stock_future']*4,
        'raw_source_state':['source_empty','missing_receipt','complete_data_exists','source_empty'],
        'official_outright_class':['positive','positive','positive','unknown'],
        'outright_volume':[1,2,10,None]}).write_csv(file)
    assert candidate_plan(file,participation=.5)['product'].to_list()==['CDF']
    assert candidate_plan(file,participation=1.)['product'].to_list()==['DDF','CDF']


def test_final_builder_failure_overrules_complete_raw_inventory(tmp_path):
    file=tmp_path/'final.parquet'
    pl.DataFrame({'date':[DAY],'physical_contract':['MTX:202004W1'],
        'product':['MTX'],'asset_class':['index_future'],
        'raw_source_state':['complete_data_exists'],'status':['unverified_calendar'],
        'outright_volume':[100]}).write_parquet(file)
    assert candidate_plan(file,participation=.5)['physical_contract'].to_list()==['MTX:202004W1']


def test_final_builder_unknown_capacity_requires_evidence(tmp_path):
    file=tmp_path/'final.parquet'
    pl.DataFrame({'date':[DAY],'physical_contract':['MTX:202004W1'],
        'product':['MTX'],'asset_class':['index_future'],'status':['unverified_calendar'],
        'outright_volume':[None]}).write_parquet(file)
    with pytest.raises(ValueError,match='pending official capacity'):
        candidate_plan(file,participation=.5)


def test_final_gap_official_proof_requires_matching_hash(tmp_path):
    file=tmp_path/'final.parquet'
    pl.DataFrame({'date':[DAY],'physical_contract':['MTX:202004W1'],
        'product':['MTX'],'asset_class':['index_future'],'status':['unverified_calendar']}).write_parquet(file)
    evidence=tmp_path/'official_evidence.parquet'
    pl.DataFrame({'date':[DAY],'physical_contract':['MTX:202004W1'],'outright_volume':[10]}).write_parquet(evidence)
    receipt=tmp_path/'official_evidence_manifest.json'
    atomic_write_json(receipt,dict(sha256='0'*64))
    with pytest.raises(ValueError,match='SHA mismatch'):
        candidate_plan(file,participation=.5,official_evidence=evidence)
    atomic_write_json(receipt,dict(sha256=sha256_file(evidence)))
    assert candidate_plan(file,participation=.5,official_evidence=evidence).height==1
