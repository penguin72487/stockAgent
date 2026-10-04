from datetime import UTC, date, datetime, timedelta
import json

import pytest

from downloader import download_finmind_complement as worker
from downloader import finmind_history_refresh as refresh
from downloader import finmind_supplemental as supplemental

NOW = datetime(2026, 9, 29, 14, tzinfo=UTC)


def row(day, value=1):
    return {'date': day, 'stock_id': 'BRK-A', 'Close': value, 'Adj_Close': value}


def test_alias_migration_is_idempotent_and_preserves_evidence(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [('USStockPrice', 'BRK/A', 'history', 'id_history', 4)])
        conn.execute("UPDATE tasks SET state='invalid_request',error_code='provider_bad_request'")
        assert refresh.migrate_us_aliases(conn) == 1
        assert refresh.migrate_us_aliases(conn) == 0
        assert conn.execute("SELECT state FROM tasks WHERE data_id='BRK/A'").fetchone() == ('identifier_alias',)
        assert conn.execute("SELECT state,priority FROM tasks WHERE data_id='BRK-A'").fetchone() == ('pending', 0)
        evidence = json.loads(conn.execute('SELECT prior_task_json FROM finmind_identifier_aliases').fetchone()[0])
        assert evidence['state'] == 'invalid_request'
        assert evidence['error_code'] == 'provider_bad_request'


@pytest.mark.parametrize('source,expected', [('BRK/A', 'BRK-A'), ('BRK.B', 'BRK.B'), ('BP.L', 'BP.L'), ('RAC/WS', 'RAC-WS')])
def test_only_documented_slash_identity_shape_is_converted(source, expected):
    assert refresh.canonical_us_id(source) == expected


def test_history_tail_preserves_older_rows_and_replaces_overlap(tmp_path):
    task = worker.Task('USStockPrice', 'BRK-A', 'history', 'id_history', 0, 'complete')
    old = [row('2014-01-02'), row('2026-09-24', 2), row('2026-09-25', 3)]
    worker._store(tmp_path, task, old, NOW - timedelta(days=1))
    receipt, loaded = refresh.read_baseline(tmp_path, task)
    plan = refresh.request_plan(receipt, NOW, NOW.date())
    assert plan['request_start_date'] == '2026-09-18'
    result = refresh.merge_response(loaded, [row('2026-09-24', 4), row('2026-09-28', 5)], 'BRK-A', plan)
    assert result == [row('2014-01-02'), row('2026-09-24', 4), row('2026-09-28', 5)]
    assert refresh.adjusted_history_changed(loaded, [row('2026-09-24', 4)])
    assert not refresh.adjusted_history_changed(loaded, [row('2026-09-24', 2)])


def test_weekly_full_refresh_and_future_clock_fail_safe():
    for age in (timedelta(days=8), timedelta(days=-1)):
        receipt = {'fetched_at_utc': (NOW - age).isoformat(), 'source_last_date': '2026-09-25'}
        assert refresh.request_plan(receipt, NOW, NOW.date())['query_shape'] == 'per_id_full_history'


@pytest.mark.parametrize('incoming', [[row('2026-09-29'), row('2026-09-29')],
                                    [{'date': '2026-09-29', 'stock_id': 'OTHER'}],
                                    [row('2026-10-01')]])
def test_invalid_tail_cannot_replace_history(incoming):
    plan = refresh.request_plan({'fetched_at_utc': NOW.isoformat(), 'source_last_date': '2026-09-25'}, NOW, NOW.date())
    with pytest.raises(ValueError):
        refresh.merge_response([row('2026-09-25')], incoming, 'BRK-A', plan)


def test_empty_incremental_retains_all_verified_history_and_original_dates():
    old = [row('2014-01-02'), row('2026-09-25', 3)]
    plan = refresh.request_plan({'fetched_at_utc': NOW.isoformat(), 'source_last_date': '2026-09-25'}, NOW, NOW.date())
    assert refresh.merge_response(old, [], 'BRK-A', plan) == old
    assert plan['empty_response_policy'] == 'retain_verified_baseline_no_new_observation'
    assert plan['history_refresh_contract_version'] == 2


def test_empty_full_refetch_never_erases_verified_history():
    plan = refresh.request_plan({}, NOW, NOW.date())
    with pytest.raises(ValueError, match='unexpected_empty_after_nonempty'):
        refresh.merge_response([row('2026-09-25')], [], 'BRK-A', plan)


def test_empty_incremental_does_not_bless_invalid_old_values():
    plan = refresh.request_plan({'fetched_at_utc': NOW.isoformat(), 'source_last_date': '2026-09-25'}, NOW, NOW.date())
    with pytest.raises(ValueError, match='history_wrong_data_id'):
        refresh.merge_response([{'date': '2026-09-25', 'stock_id': 'WRONG'}], [], 'BRK-A', plan)


def test_unexpected_empty_preserves_existing_head_even_without_correction(tmp_path):
    task = worker.Task('USStockPrice', 'BRK-A', 'history', 'id_history', 0, 'complete')
    receipt = worker._store(tmp_path, task, [row('2026-09-25')], NOW)
    path = tmp_path / receipt['receipt_path']
    original = path.read_bytes()
    with pytest.raises(worker.SourceError, match='unexpected_empty_after_nonempty'):
        worker._store(tmp_path, task, [], NOW)
    assert path.read_bytes() == original


def test_daily_history_does_not_wait_thirty_days():
    task = worker.Task('USStockPrice', 'BRK-A', 'history', 'id_history', 0, 'complete')
    due = datetime.fromisoformat(worker._next_refresh(task, NOW, empty=False))
    assert due == datetime(2026, 9, 30, 0, 0, tzinfo=UTC)


def test_supplemental_special_routes_and_signed_spread():
    endpoint, params, _ = supplemental.request_contract('TaiwanStockTradingDailyReport', '2330', '2026-09-29', NOW.date())
    assert endpoint.endswith('/taiwan_stock_trading_daily_report')
    assert params == {'data_id': '2330', 'date': '2026-09-29'}
    with pytest.raises(ValueError, match='derived_from_verified'):
        supplemental.request_contract('TaiwanStockTradingDailyReportSecIdAgg', '2330', '2021-06-01', NOW.date())
    supplemental.validate_response('TaiwanFuturesSpreadTick', '', '2026-09-29', NOW.date(),
                                   [{'date': '2026-09-29', 'futures_id': 'TX', 'price': -123}])


def test_frontiers_bound_queue_growth_and_continue_every_intervening_day(tmp_path, monkeypatch):
    spec = supplemental.SOURCES['TaiwanStockPriceTick']
    monkeypatch.setattr(supplemental, 'SOURCES', {'TaiwanStockPriceTick': spec})
    monkeypatch.setattr(supplemental, 'WORKING_SET', 2)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        stats = supplemental.seed(conn, {'stocks': ['2330', '2317', '0050']}, NOW)
        assert conn.execute('SELECT COUNT(*) FROM tasks').fetchone()[0] == 2
        assert stats['TaiwanStockPriceTick']['unseeded_partition_candidates'] > 1000
        supplemental.seed(conn, {'stocks': ['2330', '2317', '0050']}, NOW)
        assert conn.execute('SELECT COUNT(*) FROM tasks').fetchone()[0] == 2
        conn.execute("UPDATE tasks SET state='complete'")
        supplemental.seed(conn, {'stocks': ['2330', '2317', '0050']}, NOW + timedelta(days=3))
        dates = {row[0] for row in conn.execute("SELECT partition FROM tasks WHERE state='pending'")}
        assert dates == {'2026-09-30'}  # No silent leap to October 2.
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone()[0] == 2


def test_month_frontier_covers_first_partial_month(tmp_path, monkeypatch):
    name = 'TaiwanStockTradingDailyReportSecIdAgg'
    monkeypatch.setattr(supplemental, 'SOURCES', {name: supplemental.Source('stocks', date(2021,6,30), 'month', 3, 21)})
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        stats = supplemental.seed(conn, {'stocks': ['2330']}, datetime(2021, 6, 30, 14, tzinfo=UTC))
        assert conn.execute('SELECT partition FROM tasks').fetchone() == ('2021-06-01',)
        assert stats[name]['unseeded_partition_candidates'] == 0


def test_bond_histories_are_not_duplicate_monthly_or_daily_requests(tmp_path, monkeypatch):
    name = 'TaiwanAssetSwapFixedIncomeDaily'
    monkeypatch.setattr(supplemental, 'SOURCES', {name: supplemental.SOURCES[name]})
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        for _ in range(2):
            supplemental.seed(conn, {'bonds': ['17172', '13166']}, NOW)
        assert conn.execute('SELECT COUNT(*) FROM tasks').fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE partition='history'").fetchone()[0] == 2


def test_broker_aggregate_uses_verified_parent_not_another_network_call(tmp_path):
    parent = worker.Task('TaiwanStockTradingDailyReport', '2330', '2026-09-24', 'id_day', 8, 'pending')
    raw = [{'date': '2026-09-24', 'stock_id': '2330', 'securities_trader_id': '1020', 'securities_trader': '合庫',
            'price': 100, 'buy': 20, 'sell': 0},
           {'date': '2026-09-24', 'stock_id': '2330', 'securities_trader_id': '1020', 'securities_trader': '合庫',
            'price': 103, 'buy': 10, 'sell': 2}]
    with worker._db(tmp_path/'queue.sqlite3') as conn:
        worker._add_tasks(conn, [(parent.dataset,parent.data_id,parent.partition,parent.kind,parent.priority)])
        receipt = worker._store(tmp_path,parent,raw,NOW)
        worker._save_result(conn,parent,receipt,NOW)
        child = worker._next_task(conn,NOW,incremental_only=True)
        assert child.dataset == 'TaiwanStockTradingDailyReportSecIdAgg'
        output, metadata = worker._derive_broker_aggregate(tmp_path,child)
        assert output[0]['buy_volume'] == 30
        assert output[0]['buy_price'] == 101
        assert output[0]['sell_price'] == 103
        assert metadata['request_count'] == 0
        assert metadata['parent_sha256'] == receipt['sha256']
        (tmp_path/receipt['parquet_path']).write_bytes(b'bad')
        with pytest.raises(worker.SourceError,match='invalid_broker_parent'):
            worker._derive_broker_aggregate(tmp_path,child)


def test_official_cash_closures_do_not_prune_futures_night_sessions(tmp_path,monkeypatch):
    from types import SimpleNamespace
    def closed(_day):
        return SimpleNamespace(status='closed', reason='receipt-verified TWSE TAIEX historical non-session')
    names = ('TaiwanStockPriceTick','TaiwanFuturesTick')
    monkeypatch.setattr(supplemental,'SOURCES',{name:supplemental.SOURCES[name] for name in names})
    with worker._db(tmp_path/'queue.sqlite3') as conn:
        supplemental.seed(conn,{'stocks':['2330'],'futures':['MTX']},NOW,day_decision=closed)
        assert dict(conn.execute('SELECT dataset,state FROM tasks')) == {
            'TaiwanStockPriceTick':'non_session','TaiwanFuturesTick':'pending'}


def test_due_overseas_refresh_preempts_history_without_a_permanent_share(tmp_path):
    from downloader.finmind_scheduling import fixed_incremental_demand
    from types import SimpleNamespace
    root = tmp_path/'data_finmind'
    root.mkdir()
    (root/'account_status.json').write_text(json.dumps({'official_requests_per_hour':6000}))
    with worker._db(root/'complement/queue.sqlite3') as conn:
        worker._add_tasks(conn,[('USStockPrice',str(i),'history','id_history',0) for i in range(7000)])
    demand=fixed_incremental_demand(root,NOW,sources=(),day_decision=lambda *_a,**_k:SimpleNamespace(status='closed'))
    assert demand == 7002  # 7000 actually due + missing Free calendar/master.
    with worker._db(root/'complement/queue.sqlite3') as conn:
        conn.execute("UPDATE tasks SET next_attempt_at_utc=?", ((NOW + timedelta(hours=2)).isoformat(),))
    assert fixed_incremental_demand(root, NOW, sources=()) == 2


def test_snapshot_endpoint_is_explicit_not_a_fake_dataset_request(tmp_path,monkeypatch):
    seen={}
    def fetch(*args,**kwargs):
        seen.update(params=args[5],**kwargs)
        return []
    monkeypatch.setattr(worker,'_fetch_rows',fetch)
    task=worker.Task('taiwan_stock_tick_snapshot','','latest','snapshot',0,'pending')
    worker._request(None,None,tmp_path,task,'unused',today=NOW.date())
    assert seen['params'] == {'data_id':''}
    assert seen['endpoint'].endswith('/taiwan_stock_tick_snapshot')


def test_warrant_broker_query_is_not_a_stock_id_query():
    endpoint,params,_=supplemental.request_contract('TaiwanStockWarrantTradingDailyReport','5920','2026-09-24',NOW.date())
    assert endpoint.endswith('/taiwan_stock_warrant_trading_daily_report')
    assert params == {'securities_trader_id':'5920','date':'2026-09-24'}


def test_taiex_minute_history_does_not_inherit_stock_2019_floor():
    assert supplemental.history_floor('TaiwanStockKBar','TAIEX') == date(2005,1,3)
    assert supplemental.history_floor('TaiwanStockKBar','2330') == date(2019,1,1)


def test_live_derivative_master_separates_adjusted_options_and_futures(tmp_path):
    task=worker.Task('TaiwanFutOptTickInfo','','latest','snapshot',0,'pending')
    worker._store(tmp_path,task,[{'code':'NB1C7','callput':'-'},
                               {'code':'CDA21500V6','callput':'賣權(調整後)'},
                               {'code':'TXO20000I6','callput':'買權'}],NOW)
    assert worker._snapshot_derivative_products(tmp_path) == {
        'taiwan_futures_snapshot':['NB1'],'taiwan_options_snapshot':['CDA','TXO']}


def test_expired_live_quote_is_empty_capture_not_a_destroyed_history(tmp_path):
    task=worker.Task('taiwan_futures_snapshot','TXF','latest','snapshot',0,'pending')
    old=worker._store(tmp_path,task,[{'date':'2026-09-24','futures_id':'TXFI6','close':20000}],NOW)
    new=worker._store(tmp_path,task,[],NOW+timedelta(days=1))
    assert new['status']=='observed_empty'
    assert (tmp_path/old['parquet_path']).exists()
    assert list((tmp_path/'receipt_history').rglob('*.json'))
