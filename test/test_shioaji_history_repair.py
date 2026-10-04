from contextlib import nullcontext
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from dataclasses import replace

import polars as pl
import pytest

from downloader import download_shioaji_historical_market_data as exact
from downloader import download_shioaji_tx_futures_ticks as continuous
from downloader.shioaji_history_repair import (
    FuturesActivity, futures_date_is_closed, latest_completed_futures_session, load_futures_activity,
    publication_ready, recent_login_waiter, retry_due, retry_metadata,
)
from scripts.export_shioaji_futures_products import export_inventory


def test_recent_login_waiter_only_accepts_a_fresh_lock_wait_receipt(tmp_path):
    path = tmp_path / 'scheduler.json'
    now = datetime(2026, 9, 24, 0, 0, tzinfo=UTC)
    assert not recent_login_waiter(path, now=now)
    payload = {'state': 'waiting', 'reason': 'history_login_slot_busy',
               'observed_at_utc': (now - timedelta(seconds=60)).isoformat()}
    path.write_text(json.dumps(payload))
    assert recent_login_waiter(path, now=now)
    assert not recent_login_waiter(path, now=now + timedelta(seconds=121))
    path.write_text(json.dumps({**payload, 'reason': 'live_connection_reservation'}))
    assert not recent_login_waiter(path, now=now)
    path.write_text(json.dumps({**payload, 'state': 'running'}))
    assert not recent_login_waiter(path, now=now)
    path.write_text('{broken')
    assert not recent_login_waiter(path, now=now)


def test_bounded_history_batch_does_not_duplicate_its_final_receipt_audit():
    check = exact._needs_intermediate_summary
    assert check(1, max_queries=500, pending_tasks=True)
    assert check(250, max_queries=500, pending_tasks=True)
    assert not check(100, max_queries=500, pending_tasks=True)
    assert not check(400, max_queries=500, pending_tasks=True)
    assert not check(500, max_queries=500, pending_tasks=True)
    assert not check(250, max_queries=0, pending_tasks=False)
    assert check(250, max_queries=0, pending_tasks=True)
    assert not check(101, max_queries=500, pending_tasks=True)


def test_task_planning_reuses_verified_kbar_dates_without_second_scan(
    tmp_path, monkeypatch,
):
    row = SimpleNamespace(
        collection='exact_futures', priority=2, asset_class='futures', code='TXFJ6',
        begin_date=date(2026, 9, 1), end_date=date(2026, 9, 2),
    )
    calls = []

    def receipt(_path, _data_path, *, method, code):
        calls.append((method, code))
        if method == 'kbars':
            return {
                'status': 'complete',
                'observed_trading_dates': ['2026-09-01', '2026-09-02'],
            }
        return None

    monkeypatch.setattr(exact, '_valid_receipt', receipt)
    monkeypatch.setattr(
        exact, 'observed_tick_dates',
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError('second KBar scan')),
    )
    tasks = exact.build_tasks(
        tmp_path, [row], chunk_days=29,
        activity=SimpleNamespace(exact_dates=lambda _row: set()),
    )
    assert [(task.method, task.start) for task in tasks] == [
        ('ticks', date(2026, 9, 2)), ('ticks', date(2026, 9, 1)),
    ]
    assert calls.count(('kbars', 'TXFJ6')) == 1


def test_receipt_validation_cache_reuses_only_unchanged_file_identities(tmp_path, monkeypatch):
    partition = tmp_path / 'start=2026-09-01_end=2026-09-02'
    partition.mkdir()
    receipt = partition / 'receipt.json'
    data = partition / 'data.parquet'
    data.write_bytes(b'old')
    payload = {
        'schema_version': exact.RECEIPT_SCHEMA_VERSION,
        'source': exact.SOURCE,
        'method': 'kbars',
        'contract': 'TXFJ6',
        'status': 'complete',
        'start': '2026-09-01',
        'end': '2026-09-02',
        'sha256': 'verified',
    }
    receipt.write_text(json.dumps(payload))
    calls = []
    monkeypatch.setattr(exact, 'verified_sha', lambda path: calls.append(path) or 'verified')
    assert exact._valid_receipt(receipt, data, method='kbars', code='TXFJ6') == payload
    assert exact._valid_receipt(receipt, data, method='kbars', code='TXFJ6') == payload
    assert len(calls) == 1
    copy = exact._valid_receipt(receipt, data, method='kbars', code='TXFJ6')
    copy['status'] = 'tampered'
    assert exact._valid_receipt(receipt, data, method='kbars', code='TXFJ6')['status'] == 'complete'
    data.write_bytes(b'changed-size')
    assert exact._valid_receipt(receipt, data, method='kbars', code='TXFJ6') == payload
    assert len(calls) == 2
    receipt.write_text(json.dumps({**payload, 'contract': 'OTHER'}))
    assert exact._valid_receipt(receipt, data, method='kbars', code='TXFJ6') is None
    receipt.unlink()
    assert exact._valid_receipt(receipt, data, method='kbars', code='TXFJ6') is None
    receipt.write_text(json.dumps(payload))
    def replace_during_validation(path):
        path.write_bytes(b'replaced-during-hash')
        return 'verified'
    monkeypatch.setattr(exact, 'verified_sha', replace_during_validation)
    assert exact._valid_receipt(receipt, data, method='kbars', code='TXFJ6') is None


def test_empty_replies_expire_and_official_evidence_accelerates_retry():
    now = datetime(2026, 9, 7, tzinfo=UTC)
    legacy = {'status':'source_empty'}
    assert retry_due(legacy, now=now)
    receipt = {**legacy, **retry_metadata(None, empty=True, now=now)}
    assert not retry_due(receipt, now=now + timedelta(days=2))
    assert retry_due(receipt, positive_activity=True, now=now + timedelta(days=2))
    assert retry_due(receipt, now=now + timedelta(days=7))
    for _ in range(30):
        receipt.update(retry_metadata(receipt, empty=True, positive_activity=True, now=now))
    assert datetime.fromisoformat(receipt['next_retry_at_utc']) == now + timedelta(days=7)
    assert not retry_due({'status':'complete'}, now=now + timedelta(days=100))


def test_official_targets_preserve_expiry_and_exclude_weekly_from_r1_rank(tmp_path):
    day = date(2026, 9, 4)
    path = tmp_path / 'official.parquet'
    pl.DataFrame({'date':[day]*3, 'product':['MTX']*3,
                  'contract':['202609W2','202609','202610'],
                  'resolved_last_trade_date':[date(2026,9,9),date(2026,9,16),date(2026,10,21)],
                  'shioaji_roots':['MX2,MXF']*3, 'volume':[10,20,30]}).write_parquet(path)
    activity = load_futures_activity(path, start=day, end=day)
    assert activity.continuous['MXFR1'] == {day}
    assert activity.continuous['MXFR2'] == {day}
    row = SimpleNamespace(security_type='FUT', root='MXF', delivery_date=date(2026,9,16), begin_date=day, end_date=day)
    assert activity.exact_dates(row) == {day}
    row.delivery_date = date(2026,9,23)
    assert activity.exact_dates(row) == set()
    assert activity.provenance['sha256']


def test_stock_close_does_not_finalize_later_closing_futures(tmp_path):
    path = tmp_path / 'calendar.parquet'
    pl.DataFrame({'product':['TX','TX'], 'date':[date(2026,9,4),date(2026,9,7)]}).write_parquet(path)
    assert latest_completed_futures_session(path, datetime(2026,9,7,6,31,tzinfo=UTC)) == date(2026,9,4)
    assert latest_completed_futures_session(path, datetime(2026,9,7,8,31,tzinfo=UTC)) == date(2026,9,7)


def test_txfr1_day_session_finalizes_after_historical_query_gate_only():
    day = date(2026, 9, 17)
    before_gate = datetime(2026, 9, 17, 6, 30, tzinfo=UTC)
    after_gate = datetime(2026, 9, 17, 6, 31, tzinfo=UTC)
    all_futures_close = datetime(2026, 9, 17, 8, 30, tzinfo=UTC)
    assert not futures_date_is_closed(day, before_gate, contract='TXFR1')
    assert futures_date_is_closed(day, after_gate, contract='TXFR1')
    assert not futures_date_is_closed(day, after_gate, contract='MXFR1')
    assert futures_date_is_closed(day, all_futures_close, contract='MXFR1')


def future_row():
    return exact.HistoryContract('exact_futures',2,'FUT','futures','CDFI6','CDF','台積電','TAIFEX',
                                  date(2026,9,4),date(2026,9,4),delivery_date=date(2026,9,16))


def test_official_ticks_are_scheduled_even_when_kbars_are_empty(tmp_path):
    row = future_row()
    data_path, path = exact._kbar_paths(tmp_path,row,row.begin_date,row.end_date)
    receipt = {'schema_version':exact.RECEIPT_SCHEMA_VERSION,'source':exact.SOURCE,
               'method':'kbars','contract':row.code,'status':'source_empty','rows':0,
               'start':str(row.begin_date),'end':str(row.end_date),'observed_trading_dates':[],
               **retry_metadata(None,empty=True,positive_activity=True)}
    exact._atomic_write_json(path,receipt)
    activity = FuturesActivity(exact={('CDF','2026-09-16'):{row.begin_date}})
    tasks = exact.build_tasks(tmp_path,[row],chunk_days=29,refresh_empty=True,activity=activity)
    assert [(t.method,t.start) for t in tasks] == [('ticks',row.begin_date)]
    summary = exact._write_summary(tmp_path,[row],chunk_days=29,state='planned',usage=None,
                                   progress_path=tmp_path/'progress.json',persist=False,
                                   refresh_empty=True,activity=activity)
    assert summary['tick_dates'] == 1 and summary['pending_queries'] == 1
    assert summary['positive_activity_empty_queries'] == 1
    assert summary['query_receipt_state'] == 'partial'
    assert not data_path.exists()


def test_query_identity_cannot_be_reused_for_another_partition(tmp_path):
    row = future_row()
    data_path, path = exact._kbar_paths(tmp_path,row,row.begin_date,row.end_date)
    exact._atomic_write_json(path,{'schema_version':2,'source':exact.SOURCE,'method':'kbars',
                                   'contract':row.code,'status':'source_empty',
                                   'start':'2026-09-03','end':'2026-09-03'})
    assert exact._valid_receipt(path,data_path,method='kbars',code=row.code) is None


def test_quota_empty_response_does_not_create_success_receipt(tmp_path,monkeypatch):
    row = future_row()
    task = exact.build_tasks(tmp_path,[row],chunk_days=29,kbars_only=True)[0]
    payload = SimpleNamespace(dict=lambda:{field:[] for field in ('ts','Open','High','Low','Close','Volume','Amount')})
    usage = iter([1, 90])
    calls = []
    def empty_response(**kwargs):
        calls.append(1)
        return payload
    api = SimpleNamespace(contracts=SimpleNamespace(get=lambda _:object()), kbars=empty_response,
                          usage=lambda:SimpleNamespace(bytes=next(usage),limit_bytes=100))
    monkeypatch.setattr(exact,'shioaji_query',lambda *a,**kw:nullcontext(lambda _:None))
    with pytest.raises(exact.TrafficBudgetReached):
        exact._query_task(api,task,output_root=tmp_path,timeout_ms=100,retries=0,retry_backoff=0,
                          rate_limiter=SimpleNamespace(wait=lambda:None),max_traffic_fraction=0.9)
    assert len(calls) == 1
    assert not exact._kbar_paths(tmp_path,row,row.begin_date,row.end_date)[1].exists()


def test_interrupt_is_not_retried():
    calls=[]
    def interrupted():
        calls.append(1)
        raise KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        exact._query_with_retries(interrupted,retries=3,retry_backoff=0)
    assert len(calls) == 1


def test_retry_rechecks_protected_window_before_another_api_request(tmp_path, monkeypatch):
    row = future_row()
    task = exact.build_tasks(tmp_path, [row], chunk_days=29, kbars_only=True)[0]
    protected = iter([False, True])
    monkeypatch.setattr(exact, 'historical_query_is_protected', lambda: next(protected))
    monkeypatch.setattr(exact, 'shioaji_query', lambda *a, **kw: nullcontext(lambda _: None))
    calls = []
    def failed(**kwargs):
        calls.append(1)
        raise TimeoutError('transport timeout')
    api = SimpleNamespace(contracts=SimpleNamespace(get=lambda _: object()), kbars=failed,
                          usage=lambda: SimpleNamespace(bytes=1, limit_bytes=100))
    with pytest.raises(exact.HistoricalWindowReached):
        exact._query_task(api, task, output_root=tmp_path, timeout_ms=100, retries=2,
                          retry_backoff=0, rate_limiter=SimpleNamespace(wait=lambda: None),
                          allow_market_hours=False)
    assert len(calls) == 1
    assert not exact._kbar_paths(tmp_path, row, row.begin_date, row.end_date)[1].exists()


def test_partial_session_receipt_is_not_reused_as_a_completed_session(tmp_path):
    row = future_row()
    data_path, path = exact._tick_paths(tmp_path, row, row.begin_date)
    exact._atomic_write_json(path, {'schema_version': exact.RECEIPT_SCHEMA_VERSION,
                                   'source': exact.SOURCE, 'method': 'ticks', 'contract': row.code,
                                   'trading_date': str(row.begin_date), 'status': 'source_empty',
                                   'session_finalized': False})
    assert exact._valid_receipt(path, data_path, method='ticks', code=row.code) is None


def test_partial_kbar_retry_preserves_valid_minutes_and_has_cooldown(tmp_path, monkeypatch):
    row = replace(future_row(), begin_date=date(2026, 9, 3))
    task = exact.build_tasks(tmp_path, [row], chunk_days=29, kbars_only=True)[0]
    ts = int((datetime(2026, 9, 3, 9) - datetime(1970, 1, 1)).total_seconds() * 1e9)
    payload = {name: [value] for name, value in {'ts':ts, 'Open':100., 'High':100.,
               'Low':100., 'Close':100., 'Volume':1, 'Amount':100.}.items()}
    frame, observed = exact._kbar_frame(payload, row=row)
    dp, rp = exact._kbar_paths(tmp_path, row, row.begin_date, row.end_date)
    output = exact._write_parquet_atomic(frame, dp)
    exact._atomic_write_json(rp, {'schema_version':exact.RECEIPT_SCHEMA_VERSION, 'source':exact.SOURCE,
        'method':'kbars', 'contract':row.code, 'start':str(row.begin_date), 'end':str(row.end_date),
        'status':'complete', 'rows':1, 'observed_trading_dates':[str(day) for day in observed], **output})
    activity = FuturesActivity(exact={('CDF','2026-09-16'):{row.begin_date,row.end_date}})
    assert len(exact.build_tasks(tmp_path,[row],chunk_days=29,kbars_only=True,refresh_empty=True,activity=activity)) == 1
    api = SimpleNamespace(contracts=SimpleNamespace(get=lambda _: object()),
                          kbars=lambda **_: {name:[] for name in payload})
    monkeypatch.setattr(exact,'shioaji_query',lambda *a,**kw:nullcontext(lambda _:None))
    receipt, _ = exact._query_task(api,task,output_root=tmp_path,timeout_ms=100,retries=0,
        retry_backoff=0,rate_limiter=SimpleNamespace(wait=lambda:None),activity=activity,kbars_only=True)
    assert receipt['status'] == 'complete' and receipt['rows'] == 1
    assert receipt['latest_response_rows'] == 0
    assert pl.read_parquet(dp).equals(frame)
    assert receipt['official_activity_missing_dates'] == [str(row.end_date)]
    assert exact.build_tasks(tmp_path,[row],chunk_days=29,kbars_only=True,refresh_empty=True,activity=activity) == []
    summary = exact._write_summary(tmp_path,[row],chunk_days=29,state='running',usage=None,
        progress_path=tmp_path/'progress.json',persist=False,kbars_only=True,refresh_empty=True,activity=activity)
    assert summary['state'] == 'waiting_source' and summary['partial_kbar_chunks'] == 1
    assert not publication_ready(summary,continuous=False)


def test_catalog_refresh_retains_removed_aliases_and_adds_new_ones(tmp_path):
    pl.DataFrame([{'priority':1,'root':'OLD','product_name':'old','tenor':'R1','contract':'OLDR1'}]).write_csv(tmp_path/'continuous_contracts.csv')
    api=SimpleNamespace(contracts=SimpleNamespace(futures_roots=lambda:[('NEW','New')],
        futures=lambda _: [SimpleNamespace(base=SimpleNamespace(code='NEWR1'))]))
    manifest=export_inventory(api,tmp_path)
    rows={r['contract']:r for r in pl.read_csv(tmp_path/'continuous_contracts.csv').to_dicts()}
    assert rows['OLDR1']['active_selection'] is False
    assert rows['NEWR1']['active_selection'] is True
    assert manifest['continuous_contracts'] == 2 and manifest['current_continuous_contracts'] == 1
    before=(tmp_path/'continuous_contracts.csv').read_bytes()
    api.contracts.futures_roots=lambda:[]
    with pytest.raises(RuntimeError,match='empty futures catalog'):
        export_inventory(api,tmp_path)
    assert (tmp_path/'continuous_contracts.csv').read_bytes() == before


def test_empty_futures_alias_catalog_cannot_publish_a_vacuous_complete_sweep(tmp_path, monkeypatch):
    aliases = tmp_path / 'aliases.csv'
    pl.DataFrame(schema={'priority': pl.Int64, 'contract': pl.String}).write_csv(aliases)
    day = date(2026, 9, 4)
    args = SimpleNamespace(
        dry_run=True, start_date=str(day), end_date=str(day),
        dates_per_contract=1, empty_probes_per_contract=0, max_dates=1,
        calendar_path=tmp_path / 'calendar.parquet', refresh_empty=False,
        refresh_inventory=False,
        contracts_file=aliases, batch_receipt=tmp_path / 'batch.json',
    )
    monkeypatch.setattr(continuous, '_calendar', lambda *a: [day])
    with pytest.raises(RuntimeError, match='empty futures alias catalog'):
        continuous._run_batch(args)


def test_continuous_batch_uses_one_login_and_then_reuses_verified_data(tmp_path,monkeypatch):
    day=date(2026,9,4)
    aliases=tmp_path/'aliases.csv'
    pl.DataFrame({'priority':[1,2], 'contract':['CAFR1','CDFR1']}).write_csv(aliases)
    monkeypatch.setattr(sys,'argv',['collector','--contracts-file',str(aliases),'--history-root',str(tmp_path/'history'),
        '--calendar-path',str(tmp_path/'calendar'),'--start-date',str(day),'--end-date',str(day),
        '--batch-receipt',str(tmp_path/'batch.json'),'--max-dates','1'])
    monkeypatch.setattr(continuous,'_calendar',lambda *a:[day])
    monkeypatch.setattr(continuous,'_taiwan_market_hours_now',lambda:False)
    monkeypatch.setattr(continuous,'shioaji_query',lambda *a,**kw:nullcontext(lambda _:None))
    monkeypatch.setattr(continuous,'SharedRateLimiter',lambda *a,**kw:SimpleNamespace(wait=lambda:None))
    calls=[]
    tick=SimpleNamespace(ts=[1788482760000000000],close=[100.],volume=[1],bid_price=[99.],
                         bid_volume=[1],ask_price=[101.],ask_volume=[1],tick_type=[1])
    class Api:
        contracts=SimpleNamespace(get=lambda _:SimpleNamespace(target_code='ACTUAL'))
        def __init__(self,**kw):pass
        def set_event_callback(self,cb):pass
        def login(self,**kw):calls.append('login')
        def logout(self):calls.append('logout')
        def usage(self):return SimpleNamespace(bytes=1,limit_bytes=1000)
        def ticks(self,**kw):calls.append('ticks');return tick
    monkeypatch.setitem(sys.modules,'shioaji',SimpleNamespace(Shioaji=Api))
    monkeypatch.setenv('SHIOAJI_API_KEY','test-key')
    monkeypatch.setenv('SHIOAJI_SECRET_KEY','test-secret')
    assert continuous.main() == 0
    assert calls.count('login') == 1 and calls.count('logout') == 1 and calls.count('ticks') == 1
    payload=json.loads((tmp_path/'batch.json').read_text())
    assert payload['status'] == 'batch_partial'
    assert payload['scanned_contracts'] == 1 and payload['total_contracts'] == 2
    assert payload['coverage_scope'] == 'scanned_contracts_only'
    assert payload['query_budget_reached'] is True
    assert payload['current_query_sweep_complete'] is False
    assert not publication_ready(payload,continuous=True)
    calls.clear()
    assert continuous.main() == 0
    assert calls.count('login') == 1 and calls.count('logout') == 1 and calls.count('ticks') == 1
    payload=json.loads((tmp_path/'batch.json').read_text())
    assert payload['status'] == 'batch_finished'
    assert payload['coverage_scope'] == 'full_catalog'
    assert payload['current_query_sweep_complete'] is True
    assert publication_ready(payload,continuous=True)
    calls.clear()
    assert continuous.main() == 0
    assert calls == []


def test_unavailable_contract_does_not_erase_preserved_data_counts(tmp_path,monkeypatch):
    monkeypatch.setattr(continuous,'_write_manifest',lambda *a,**k:{
        'resolved_trading_dates':1,'complete_trading_dates':1,'source_empty_trading_dates':0,
        'missing_trading_dates':[],'rows':42,'bytes':100})
    receipt=continuous._write_contract_unavailable_manifest(tmp_path,contract='CAFR1',expected=[date(2026,9,4)])
    assert receipt['rows'] == 42 and receipt['status'] == 'contract_unavailable'
    assert receipt['next_retry_at_utc']
