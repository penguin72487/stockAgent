from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
import csv
import json

import polars as pl
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from downloader.repair_shioaji_futures_minute_gaps import repair_tasks
from stockagent.data.tw_stock_futures_repair import official_day_evidence, ExactMinuteRecovery, NO_TRADE, NO_CAPACITY, apply_block_evidence
from stockagent.data.tw_stock_futures_history import MINUTE_SCHEMA


def test_repair_tasks_keep_adjusted_and_unobserved_held_contract_days():
    inventory = pl.DataFrame({'product':['PLF', 'PL1'], 'contract':['202410']*2})
    gaps = pl.DataFrame({'date':[date(2024,9,24), date(2024,9,25), date(2024,9,25)],
                         'physical_contract':['PL1:202410', 'PL1:202410', 'PL1:202410']})
    tasks = repair_tasks(inventory, gaps)
    assert len(tasks) == 1
    assert tasks[0].code == 'PL1J4'
    assert tasks[0].contract.delivery_month == '202410'
    assert (tasks[0].start, tasks[0].end) == (date(2024,9,24), date(2024,9,25))
    assert tasks[0].method == 'kbars'
    assert len(repair_tasks(inventory, gaps, method='ticks')) == 2


def test_repair_tasks_never_silently_drop_unknown_identity():
    inventory = pl.DataFrame({'physical_contract':['PLF:202410']})
    gaps = pl.DataFrame({'date':[date(2024,9,24)], 'physical_contract':['PL1:202410']})
    with pytest.raises(ValueError, match='absent from official inventory'):
        repair_tasks(inventory, gaps)


def test_repair_tasks_validate_identity_and_bounded_archived_queries():
    inventory = pl.DataFrame({'physical_contract':['PL1:202410']})
    gaps = pl.DataFrame({'date':[date(2024,9,1), date(2024,9,30)], 'physical_contract':['PL1:202410']*2})
    assert len(repair_tasks(inventory, gaps)) == 2
    with pytest.raises(ValueError, match='identity mismatch'):
        repair_tasks(inventory.with_columns(pl.lit('PLF').alias('product'), pl.lit('202410').alias('contract')), gaps)
    with pytest.raises(ValueError, match='query range'):
        repair_tasks(inventory, gaps.with_columns(pl.lit(date(2026,9,24)).alias('date')))
    assert repair_tasks(inventory, gaps.head(0)) == []


def test_official_zero_spread_only_and_absence_are_independent_of_carried_prices(tmp_path):
    raw = tmp_path/'all.csv'
    columns=['交易日期','契約','到期月份(週別)','成交量','開盤價','最高價','最低價','收盤價','交易時段','結算價']
    with raw.open('w',encoding='cp950',newline='') as f:
        writer=csv.writer(f);writer.writerow(columns)
        writer.writerows([
            ['2020/03/23','CDF','202004',4,100,'-','-',100,'一般',100.5],
            ['2020/03/23','CDF','202004/202005',4,1,1,1,1,'一般','-'],
            ['2020/03/23','DDF','202004',0,'-','-','-','-','一般',99],
            ['2020/03/23','FCF','202004',7,100,101,99,100,'一般',100],
        ])
    manifest=tmp_path/'source.json'
    atomic_write_json(manifest,dict(receipts=[dict(path=str(raw),sha256=sha256_file(raw))]))
    keys=pl.DataFrame(dict(date=[date(2020,3,23)]*4,physical_contract=['CDF:202004','DDF:202004','FCF:202004','MYF:202004']))
    result=official_day_evidence(manifest,keys,tmp_path/'proof')
    got={r['physical_contract']:r for r in result.to_dicts()}
    assert got['CDF:202004']['official_reason']=='spread_legs_only'
    assert got['CDF:202004']['outright_volume']==0
    assert got['DDF:202004']['official_reason']=='zero_total_volume'
    assert got['MYF:202004']['official_reason']=='absent_from_complete_day'
    assert got['FCF:202004']['outright_volume']==7
    from stockagent.data.tw_stock_futures_carry import load_carry_daily_settlements
    marks = load_carry_daily_settlements(tmp_path/'proof/official_evidence.parquet')
    assert dict(marks.select('physical_contract','official_daily_settlement').iter_rows()) == {
        'CDF:202004':100.5, 'DDF:202004':99., 'FCF:202004':100.}
    # A full-calendar valuation audit must not enlarge the originally absent
    # minute-proof scope. A reported quote remains usable independently.
    from stockagent.data.tw_stock_futures_carry import load_carry_no_trade_evidence
    proof_path = tmp_path/'proof/official_evidence.parquet'
    result.with_columns(pl.lit(False).alias('no_trade_proof_eligible')).write_parquet(proof_path)
    receipt_path = proof_path.with_name('official_evidence_manifest.json')
    receipt = json.loads(receipt_path.read_text()); receipt['sha256'] = sha256_file(proof_path)
    atomic_write_json(receipt_path, receipt)
    assert load_carry_no_trade_evidence(proof_path).is_empty()
    assert load_carry_daily_settlements(proof_path).height == 3
    raw.write_bytes(raw.read_bytes()+b'\n')
    with pytest.raises(ValueError,match='SHA mismatch'):
        official_day_evidence(manifest,keys,tmp_path/'proof')


def test_repair_workspace_never_enters_cold_publication():
    catalog=json.loads(Path('configs/data_sync/packed_datasets.json').read_text())
    entry=next(e for e in catalog['datasets'] if e['dataset']=='tw-futures')
    assert 'shioaji_gap_repair' in entry['excluded_subtrees']


def recovery_fixture(tmp_path, *, volume=0, empty=True):
    day=date(2020,3,23)
    atomic_write_json(tmp_path/'repair_plan.json',dict(tasks=[dict(physical_contract='CDF:202004',code='CDFD0',start=str(day),end=str(day))]))
    folder=tmp_path/'contracts/futures/CDFD0/kbars/start=2020-03-23_end=2020-03-23'
    data=folder/'data.parquet'
    receipt=dict(schema_version=2,source='shioaji_historical_market_data_v2',method='kbars',contract='CDFD0',
                 status='source_empty' if empty else 'complete', rows=0 if empty else 1,
                 start=str(day),end=str(day),finished_at_utc='2026-09-08T16:00:00Z',session_finalized=True)
    if not empty:
        ts=int((datetime(2020,3,23,8,46)-datetime(1970,1,1)).total_seconds()*1e9)
        raw=pl.DataFrame(dict(ts=[ts],trading_date=[day],query_contract=['CDFD0'],security_type=['FUT'],
                             Open=[100.],High=[100.],Low=[100.],Close=[100.],Volume=[1],Amount=[100.]))
        atomic_write_parquet(data,raw);receipt['sha256']=sha256_file(data)
    atomic_write_json(folder/'receipt.json',receipt)
    official=pl.DataFrame([dict(date=day,physical_contract='CDF:202004',official_reason='zero_total_volume' if volume==0 else 'outright_volume',
                                outright_volume=volume,official_high='100',official_low='100')])
    row=dict(date=day,physical_contract='CDF:202004')
    return ExactMinuteRecovery(tmp_path,official),row,folder/'receipt.json'


def test_empty_query_is_not_zero_trading_without_official_proof(tmp_path):
    recovery,row,_=recovery_fixture(tmp_path,volume=7)
    bars,evidence=recovery.recover(row,pl.DataFrame(schema=MINUTE_SCHEMA),dict(status='missing_receipt'))
    assert bars.is_empty() and evidence['status']=='source_empty_unresolved'
    recovery,row,_=recovery_fixture(tmp_path,volume=0)
    _,evidence=recovery.recover(row,pl.DataFrame(schema=MINUTE_SCHEMA),dict(status='missing_receipt'))
    assert evidence['status']==NO_TRADE


def test_exact_month_recovers_execution_and_checks_source_contradiction(tmp_path):
    recovery,row,path=recovery_fixture(tmp_path,volume=1,empty=False)
    bars,evidence=recovery.recover(row,pl.DataFrame(schema=MINUTE_SCHEMA),dict(status='unverified_calendar'))
    assert evidence['status']=='minute_verified'
    assert bars['minute'].to_list()==[526] and bars['volume'].to_list()==[1.]
    recovery,row,_=recovery_fixture(tmp_path,volume=0,empty=False)
    _,evidence=recovery.recover(row,pl.DataFrame(schema=MINUTE_SCHEMA),dict(status='missing_receipt'))
    assert evidence['status']=='invalid_repair_source'
    payload=json.loads(path.read_text());payload['finished_at_utc']='2020-03-23T03:00:00Z'
    atomic_write_json(path,payload)
    _,evidence=recovery.recover(row,pl.DataFrame(schema=MINUTE_SCHEMA),dict(status='missing_receipt'))
    assert evidence['status']=='invalid_repair_source'


def test_archived_contract_query_is_opt_in_and_month_identity_checked(tmp_path, monkeypatch):
    from downloader.download_shioaji_historical_market_data import HistoryContract, HistoryTask, _query_task
    row=HistoryContract('exact_futures',2,'FUT','futures','CDFE0','CDF','wrong_month','TAIFEX',date(2020,3,23),date(2020,3,23),delivery_month='202004')
    task=HistoryTask(2,0,row.code,0,'kbars',row,row.begin_date,row.end_date)
    api=SimpleNamespace(contracts=SimpleNamespace(get=lambda _:None))
    kwargs=dict(output_root=tmp_path,timeout_ms=1000,retries=0,retry_backoff=0,rate_limiter=None,kbars_only=True)
    with pytest.raises(LookupError,match='current_catalog'):
        _query_task(api,task,**kwargs)
    with pytest.raises(ValueError,match='archived physical'):
        _query_task(api,task,allow_archived_contract=True,**kwargs)


def test_bad_execution_price_blocks_but_unrelated_minute_is_quarantined(tmp_path):
    recovery,row,receipt_path=recovery_fixture(tmp_path,volume=2,empty=False)
    path=receipt_path.with_name('data.parquet')
    raw=pl.read_parquet(path)
    bad=raw.with_columns(pl.col('ts')+60*60_000_000_000,pl.lit(200.).alias('High'))
    atomic_write_parquet(path,pl.concat([raw,bad]))
    receipt=json.loads(receipt_path.read_text());receipt.update(rows=2,sha256=sha256_file(path));atomic_write_json(receipt_path,receipt)
    bars,evidence=recovery.recover(row,pl.DataFrame(schema=MINUTE_SCHEMA),dict(status='missing_receipt'))
    assert evidence['status']=='minute_verified' and evidence['non_execution_quarantine']=='[586]'
    assert bars.height==1 and bars['minute'].item()==526
    atomic_write_parquet(path,raw.with_columns(pl.lit(200.).alias('High')))
    receipt.update(rows=1,sha256=sha256_file(path));atomic_write_json(receipt_path,receipt)
    _,evidence=recovery.recover(row,pl.DataFrame(schema=MINUTE_SCHEMA),dict(status='missing_receipt'))
    assert evidence['status']=='invalid_repair_source' and 'execution minute' in evidence['detail']


@pytest.mark.parametrize('status,volume', [(NO_TRADE, 0), (NO_CAPACITY, 1)])
def test_official_no_trade_tape_preserves_candidate_and_needs_independent_proof(tmp_path, status, volume):
    import numpy as np
    from test_tw_stock_futures_history import historical_fixture
    from stockagent.data.tw_stock_futures_minute import load_futures_minute_tape, validate_futures_minute_data
    from stockagent.data.tw_stock_futures_repair import REPAIR_SOURCE
    day=date(2020,3,23)
    path,keys,manifest=historical_fixture(tmp_path,day)
    coverage=pl.read_parquet(tmp_path/'coverage.parquet').with_columns(pl.lit(status).alias('status'))
    atomic_write_parquet(tmp_path/'coverage.parquet',coverage)
    official=pl.DataFrame(dict(date=[day],physical_contract=['CDF:202001'],outright_volume=[volume],official_reason=['zero_total_volume' if volume==0 else 'outright_volume'],official_day_sources=['["'+'a'*64+'"]']))
    atomic_write_parquet(tmp_path/'official_evidence.parquet',official)
    manifest.update(source_kind=REPAIR_SOURCE,repair_contract_version=1)
    for k in ('coverage','official_evidence'):
        manifest['outputs'][k]=dict(file=k+'.parquet',sha256=sha256_file(tmp_path/(k+'.parquet')))
    atomic_write_json(tmp_path/'manifest.json',manifest)
    tape,_=load_futures_minute_tape(path,keys,np.array([str(day)],dtype='datetime64[D]'),('2330',),daily_sha256='daily',fee=40,participation=.5,daily_proxy_before='2020-01-01')
    assert tape[0,0,0,0]==2000 and not tape[...,3:].any()
    if status==NO_CAPACITY:
        with pytest.raises(ValueError,match='zero integer capacity'):
            validate_futures_minute_data(path,daily_sha256='daily',daily_proxy_before='2020-01-01',participation=1.)
        with pytest.raises(ValueError,match='ceil minute capacity invalidates'):
            validate_futures_minute_data(path,daily_sha256='daily',daily_proxy_before='2020-01-01',participation=.5,capacity_rounding='ceil')
    else:
        validate_futures_minute_data(path,daily_sha256='daily',daily_proxy_before='2020-01-01',participation=.5,capacity_rounding='ceil')
    atomic_write_parquet(tmp_path/'official_evidence.parquet',official.with_columns(pl.lit(7).alias('outright_volume')))
    manifest['outputs']['official_evidence']['sha256']=sha256_file(tmp_path/'official_evidence.parquet')
    atomic_write_json(tmp_path/'manifest.json',manifest)
    with pytest.raises(ValueError,match='independent official evidence|zero integer capacity'):
        validate_futures_minute_data(path,daily_sha256='daily',daily_proxy_before='2020-01-01',participation=.5)


def test_exact_ticks_recover_empty_kbars_without_invented_vwap(tmp_path):
    from test_tw_stock_futures_history import ticks
    from stockagent.data.tw_stock_futures_history import _reuse_verified_contract
    recovery,row,_=recovery_fixture(tmp_path,volume=10)
    plan=dict(tasks=[dict(physical_contract='CDF:202004',code='CDFD0',start=str(row['date']),end=str(row['date']),method='ticks')])
    atomic_write_json(tmp_path/'repair_tick_plan.json',plan)
    folder=tmp_path/'contracts/futures/CDFD0/ticks/trading_date=2020-03-23'
    raw=ticks().with_columns(pl.lit('CDFD0').alias('query_contract'),pl.lit(100.).alias('close'))
    atomic_write_parquet(folder/'data.parquet',raw)
    atomic_write_json(folder/'receipt.json',dict(schema_version=2,source='shioaji_historical_market_data_v2',
        method='ticks',contract='CDFD0',status='complete',rows=raw.height,sha256=sha256_file(folder/'data.parquet'),
        trading_date=str(row['date']),finished_at_utc='2026-09-08T16:00:00Z',session_finalized=True))
    recovery.tick_tasks=plan['tasks']
    bars,evidence=recovery.recover(row,pl.DataFrame(schema=MINUTE_SCHEMA),dict(status='missing_receipt',date=row['date']))
    assert evidence['status']=='minute_verified' and evidence['repair_kind']=='exact_ticks'
    assert bars.filter(pl.col('minute')==526)['vwap'].item()==100.
    assert bars['volume'].sum()==10 and _reuse_verified_contract(tmp_path,evidence)


@pytest.mark.parametrize('volume,participation,expected', [
    (1, .5, NO_CAPACITY), (1, 1., 'source_empty_unresolved'),
    (2, .5, 'source_empty_unresolved'), (1, None, 'source_empty_unresolved'),
])
def test_empty_source_capacity_proof_uses_actual_integer_participation(tmp_path, volume, participation, expected):
    recovery,row,_=recovery_fixture(tmp_path,volume=volume)
    recovery.participation=participation
    bars,evidence=recovery.recover(row,pl.DataFrame(schema=MINUTE_SCHEMA),dict(status='missing_receipt'))
    assert bars.is_empty() and evidence['status']==expected


def test_ceil_repair_requires_minute_evidence_for_one_official_contract(tmp_path):
    recovery, row, _ = recovery_fixture(tmp_path, volume=1)
    recovery.participation = .5
    recovery.capacity_rounding = 'ceil'
    bars, evidence = recovery.recover(row, pl.DataFrame(schema=MINUTE_SCHEMA), dict(status='missing_receipt'))
    assert bars.is_empty()
    assert evidence['status'] == 'source_empty_unresolved'


def test_block_proof_rejects_html_wrong_hash_and_excess_quantity(tmp_path):
    recovery,_,_=recovery_fixture(tmp_path,volume=200)
    raw=tmp_path/'block.csv'
    raw.write_text('交易日期,契約,到期月份(週別),成交數量,交易時段\n2020/03/23,CDF,202004,200,一般\n',encoding='cp950')
    manifest=tmp_path/'block.json'
    proof=dict(source='taifex_official_block_volume_v1',sources=[dict(path=str(raw),sha256=sha256_file(raw),
        date='2020-03-23',url='https://www.taifex.com.tw/cht/3/dlProductOrderDown')])
    atomic_write_json(manifest,proof)
    official=pl.DataFrame(list(recovery.official.values()))
    result,_=apply_block_evidence(official,manifest)
    assert result['outright_volume'].item()==0 and result['official_reason'].item()=='spread_and_block_legs_only'
    with pytest.raises(ValueError,match='exceeds'):
        apply_block_evidence(official.with_columns(pl.lit(199).alias('outright_volume')),manifest)
    raw.write_bytes(b'<html>no data</html>')
    with pytest.raises(ValueError,match='SHA'):
        apply_block_evidence(official,manifest)
    proof['sources'][0]['sha256']=sha256_file(raw);atomic_write_json(manifest,proof)
    with pytest.raises(ValueError,match='CSV schema'):
        apply_block_evidence(official,manifest)


def test_quarantined_source_requires_explicit_matching_config(tmp_path):
    from test_tw_stock_futures_history import historical_fixture
    from stockagent.data.tw_stock_futures_minute import validate_futures_minute_data
    day=date(2020,3,23)
    path,_,manifest=historical_fixture(tmp_path,day)
    coverage=pl.read_parquet(tmp_path/'coverage.parquet').with_columns(pl.lit('source_empty_unresolved').alias('status'))
    atomic_write_parquet(tmp_path/'coverage.parquet',coverage)
    atomic_write_parquet(path,pl.DataFrame(schema=MINUTE_SCHEMA))
    manifest.update(status='complete_with_quarantine',quarantined_dates=[str(day)],covered_dates=[])
    for key in ('minutes','coverage'):
        manifest['outputs'][key]['sha256']=sha256_file(tmp_path/(key+'.parquet'))
    atomic_write_json(tmp_path/'manifest.json',manifest)
    kwargs=dict(daily_sha256='daily',daily_proxy_before='2020-01-01')
    with pytest.raises(ValueError,match='explicit quarantine'):
        validate_futures_minute_data(path,**kwargs)
    validate_futures_minute_data(path,quarantine_dates=[str(day)],**kwargs)
