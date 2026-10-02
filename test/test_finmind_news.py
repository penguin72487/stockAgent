from datetime import UTC, date, datetime, timedelta
import json

import pytest

from downloader import finmind_news as news
from downloader import download_finmind_complement as worker
from downloader.finmind_eta import milestone_workloads
from downloader.finmind_eta_telemetry import recurring_forecast


def test_news_calendar_queue_is_complete_idempotent_and_includes_holidays(tmp_path, monkeypatch):
    monkeypatch.setattr(news, 'SEARCH_FLOOR', date(2026, 9, 25))
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        news.seed(conn, date(2026, 9, 29))
        news.seed(conn, date(2026, 9, 29))
        assert conn.execute('SELECT count(*) FROM tasks').fetchone()[0] == 5
        assert conn.execute("SELECT state FROM tasks WHERE partition='2026-09-27'").fetchone() == ('pending',)
        conn.execute("UPDATE tasks SET state='complete' WHERE partition='2026-09-27'")
        news.seed(conn, date(2026, 10, 1))
        assert conn.execute('SELECT count(*) FROM tasks').fetchone()[0] == 7
        assert conn.execute("SELECT state,priority FROM tasks WHERE partition='2026-09-27'").fetchone() == ('complete', 1)
        assert conn.execute('SELECT count(*) FROM tasks WHERE priority=0').fetchone()[0] == 3


def test_news_is_whole_market_day_not_per_stock_or_range():
    params, meta = news.request_contract('2015-01-05')
    assert params == {'dataset': news.DATASET, 'start_date': '2015-01-05'}
    assert meta['provider_earliest_date_verified'] is False
    assert meta['historical_point_in_time'] is False


def article(**kwargs):
    return dict(date='2026-09-29 08:00:00', stock_id='2330', title='Title', source='Source',
                link='https://example.test/a', **kwargs)


def test_news_keeps_multi_symbol_articles_and_flags_raw_duplicates():
    row = article()
    other = {**row, 'stock_id': '2317'}
    result = news.validate_response('2026-09-29', [row, other, row])
    assert result['exact_duplicate_rows'] == 1
    assert result['rows_validated'] == 3
    assert result['grain'] == 'article_stock_association'


@pytest.mark.parametrize('patch', [{'date':'2026-09-30 01:00:00'}, {'date':'bad'},
                                  {'stock_id':2330}, {'link':'javascript:alert(1)'}])
def test_news_rejects_wrong_day_and_broken_fields(patch):
    with pytest.raises(ValueError):
        news.validate_response('2026-09-29', [{**article(), **patch}])


def test_sparse_provider_content_is_preserved_not_a_whole_day_failure():
    result = news.validate_response('2026-09-29', [{**article(), 'title': None, 'source': ''}])
    assert result['missing_optional_fields'] == {'title':1, 'source':1, 'description':1}


@pytest.mark.parametrize('missing', [None, '', '  '])
def test_missing_link_or_stock_mapping_is_raw_quality_not_a_failed_day(missing):
    rows = [article(), {**article(), 'link': missing, 'stock_id': missing}]
    before = json.dumps(rows, ensure_ascii=False)
    result = news.validate_response('2026-09-29', rows)
    assert result['rows_validated'] == 2
    assert result['missing_identity_fields'] == {'stock_id': 1, 'link': 1}
    assert result['quality_policy'] == 'sparse_metadata_preserved_not_whole_day_rejected'
    assert json.dumps(rows, ensure_ascii=False) == before


def test_receipt_stores_sparse_link_without_fabricating_a_url(tmp_path):
    now = datetime(2026, 9, 29, 15, tzinfo=UTC)
    task = worker.Task(news.DATASET, '', '2026-09-29', 'market_day', 0, 'pending')
    rows = [article(), {**article(), 'link': None}]
    _, metadata = news.request_contract(task.partition)
    metadata['validation'] = news.validate_response(task.partition, rows)
    receipt = worker._store(tmp_path, task, rows, now, request_metadata=metadata)
    assert receipt['rows'] == 2
    assert receipt['field_non_null_counts']['link'] == 1
    assert receipt['request']['news_contract_version'] == 2
    assert receipt['request']['validation']['missing_identity_fields']['link'] == 1


def test_news_incremental_clock_uses_calendar_overlap_even_empty():
    now = datetime(2026, 9, 29, 15, tzinfo=UTC)
    for empty in (True, False):
        assert datetime.fromisoformat(news.next_refresh('2026-09-27', now, empty=empty)) == now + timedelta(hours=1)
    assert datetime.fromisoformat(news.next_refresh('2014-01-01', now, empty=False)) == now + timedelta(days=365)


def test_news_receipt_keeps_timestamp_and_does_not_invent_description(tmp_path):
    now = datetime(2026, 9, 29, 15, tzinfo=UTC)
    task = worker.Task(news.DATASET, '', '2026-09-29', 'market_day', 0, 'pending')
    _, metadata = news.request_contract(task.partition)
    metadata['validation'] = news.validate_response(task.partition, [article()])
    result = worker._store(tmp_path, task, [article()], now, request_metadata=metadata)
    assert result['rows'] == 1 and result['historical_point_in_time'] is False
    assert 'description' not in result['field_non_null_counts']
    with pytest.raises(worker.SourceError, match='unexpected_empty_after_nonempty'):
        worker._store(tmp_path, task, [], now + timedelta(hours=1))


def test_future_refresh_cost_not_current_overdue_task_count(tmp_path):
    now = datetime(2026, 9, 29, tzinfo=UTC)
    for owner in ('sponsor', 'complement'):
        with worker._db(tmp_path / owner / 'queue.sqlite3') as conn:
            worker._add_tasks(conn, [('Pending', str(i), 'history', 'id_history', 0) for i in range(100)])
            worker._add_tasks(conn, [('Renewed', '', 'history', 'id_history', 0)])
            conn.execute("UPDATE tasks SET state='complete',last_attempt_at_utc=?,next_attempt_at_utc=? WHERE dataset='Renewed'",
                         (now.isoformat(), (now+timedelta(hours=24)).isoformat()))
    result = recurring_forecast(tmp_path)
    # Only configured renewal sources count; 200 overdue rows are not 200/h.
    assert 3 <= result['requests_per_hour'] < 10
    assert result['includes_current_overdue_backlog'] is False


def test_milestones_are_cumulative_and_tick_stays_last():
    names = [news.DATASET, 'TaiwanStockPrice', 'TaiwanStockKBar', 'TaiwanStockPriceTick']
    scopes = milestone_workloads({'datasets':[dict(dataset=name, current_plan_requests=10, state='observed') for name in names]})
    assert [scopes[key]['summary']['current_plan_requests'] for key in ('core','non_tick','all')] == [20,30,40]
