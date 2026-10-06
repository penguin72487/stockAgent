from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import sqlite3

import pytest

from downloader import finmind_eta_work as work


NOW = datetime(2026, 9, 27, 4, tzinfo=UTC)


def _database(root: Path, owner: str, rows: list[tuple], *, limit: int | None = None,
              disabled: bool = False) -> Path:
    path = root / owner / 'queue.sqlite3'
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE tasks(dataset TEXT,data_id TEXT,partition TEXT,kind TEXT,priority INTEGER,'
                     'state TEXT,next_attempt_at_utc TEXT)')
        conn.executemany('INSERT INTO tasks VALUES (?,?,?,?,?,?,?)', rows)
        if limit is not None:
            table = 'request_batch_limits' if owner == 'sponsor' else 'complement_year_batch_policy'
            conn.execute(f'CREATE TABLE {table}(dataset TEXT,max_partitions INTEGER)')
            conn.execute(f'INSERT INTO {table} VALUES (?,?)', (rows[0][0], limit))
        if disabled:
            conn.execute('CREATE TABLE request_batch_policy(dataset TEXT)')
            conn.execute('INSERT INTO request_batch_policy VALUES (?)', (rows[0][0],))
    return path


def _spec(dataset: str, owner: str = 'sponsor', shape: str = 'whole_market_day',
          aliases: tuple[str, ...] = ()) -> dict:
    return {'dataset': dataset, 'owners': [owner, *aliases], 'primary_owner': owner,
            'owner_contracts': {owner: {'query_shape': shape}}}


def _result(tmp_path, monkeypatch, specs):
    monkeypatch.setattr(work, '_registry', lambda: {row['dataset']: row for row in specs})
    result = work.build_finmind_workload(tmp_path, NOW)
    return result, {row['dataset']: row for row in result['datasets']}


def test_primary_owner_excludes_aliases_derived_and_retired_tasks(tmp_path, monkeypatch):
    dataset = 'TaiwanStockPrice'
    _database(tmp_path, 'sponsor', [
        (dataset, '', '2026-09-24', 'day', 0, 'pending', None),
        (dataset, '', '2020-01-02', 'day', 2, 'pending', None),
        (dataset, '', '2020-01-03', 'day', 8, 'pending', None),
        (dataset, '', '2020-01-04', 'day', 2, 'non_session', None),
        (dataset, '', '2019-01-01', 'day', 2, 'complete', '2020-01-01T00:00:00+00:00'),
        ('Derived', '', '2020-01-02', 'derived', 2, 'pending', None),
    ])
    _database(tmp_path, 'complement', [(dataset, '2330', 'history', 'id_history', 2, 'pending', None)])
    result, rows = _result(tmp_path, monkeypatch, [_spec(dataset, aliases=('complement',)),
                                                _spec('Derived', shape='derived_no_api')])
    assert rows[dataset]['required_requests'] == 2
    assert rows[dataset]['validation_requests'] == 1
    assert rows[dataset]['current_plan_requests'] == 3
    assert rows[dataset]['owner_aliases_not_added'] == ['complement']
    assert rows['Derived']['required_requests'] == 0
    assert rows['Derived']['local_derived_tasks'] == 1
    assert result['summary']['unbatched_requests'] == 3


def test_done_refresh_only_due_priority_zero_is_incremental(tmp_path, monkeypatch):
    dataset = 'TaiwanStockPrice'
    _database(tmp_path, 'sponsor', [
        (dataset, '', '2026-09-24', 'day', 0, 'complete', NOW.isoformat()),
        (dataset, '', '2026-09-25', 'day', 0, 'complete', None),
        (dataset, '', '2026-09-26', 'day', 0, 'observed_empty', (NOW + timedelta(days=1)).isoformat()),
        (dataset, '', '2020-01-03', 'day', 2, 'complete', NOW.isoformat()),
    ])
    _, rows = _result(tmp_path, monkeypatch, [_spec(dataset)])
    assert rows[dataset]['incremental_requests'] == 1
    assert rows[dataset]['backfill_requests'] == 0
    assert rows[dataset]['completed_tasks'] == 4


def test_calendar_wait_is_visible_but_neither_download_work_nor_completion(tmp_path, monkeypatch):
    dataset = 'TaiwanStockPrice'
    _database(tmp_path, 'sponsor', [(dataset, '', '2026-09-28', 'day', 0, 'calendar_wait',
                                    (NOW + timedelta(hours=1)).isoformat())])
    result, rows = _result(tmp_path, monkeypatch, [_spec(dataset)])
    assert result['summary']['current_plan_requests'] == 0
    assert rows[dataset]['calendar_wait_tasks'] == 1
    assert rows[dataset]['completed_tasks'] == rows[dataset]['blocked_tasks'] == 0


def test_sponsor_contiguous_ranges_obey_learned_limit(tmp_path, monkeypatch):
    dataset = 'TaiwanBusinessIndicator'
    _database(tmp_path, 'sponsor', [
        (dataset, '', f'{year}-01-01', 'year', 2, 'pending', None) for year in range(2010, 2016)
    ], limit=2)
    _, rows = _result(tmp_path, monkeypatch, [_spec(dataset, shape='whole_market_date_range')])
    row = rows[dataset]
    assert row['current_plan_requests'] == 3
    assert row['fastest_requests'] == 1
    assert row['unbatched_requests'] == 6
    assert row['batch_savings'] == 3


def test_sponsor_completed_holes_and_cooldown_never_join_batches(tmp_path, monkeypatch):
    dataset = 'TaiwanBusinessIndicator'
    _database(tmp_path, 'sponsor', [
        (dataset, '', '2010-01-01', 'year', 2, 'pending', None),
        (dataset, '', '2011-01-01', 'year', 2, 'complete', None),
        (dataset, '', '2012-01-01', 'year', 2, 'pending', None),
        (dataset, '', '2013-01-01', 'year', 2, 'failed', (NOW + timedelta(minutes=5)).isoformat()),
        (dataset, '', '2014-01-01', 'year', 2, 'pending', None),
    ])
    _, rows = _result(tmp_path, monkeypatch, [_spec(dataset, shape='whole_market_date_range')])
    assert rows[dataset]['current_plan_requests'] == 4
    assert rows[dataset]['cooling_tasks'] == 1
    assert rows[dataset]['max_retry_wait_seconds'] == 300


def test_disabled_contract_never_claims_batch_savings(tmp_path, monkeypatch):
    dataset = 'TaiwanBusinessIndicator'
    _database(tmp_path, 'sponsor', [
        (dataset, '', f'{year}-01-01', 'year', 2, 'pending', None) for year in range(2010, 2013)
    ], disabled=True)
    _, rows = _result(tmp_path, monkeypatch, [_spec(dataset, shape='whole_market_date_range')])
    assert rows[dataset]['fastest_requests'] == rows[dataset]['current_plan_requests'] == 3


def test_complement_years_and_ids_have_different_request_grains(tmp_path, monkeypatch):
    _database(tmp_path, 'complement', [
        ('GoldPrice', '', str(year), 'year', 1, 'pending', None) for year in range(2000, 2004)
    ] + [('InterestRate', 'FED', 'history', 'id_history', 1, 'pending', None),
         ('InterestRate', 'ECB', 'history', 'id_history', 1, 'invalid_request', None)])
    _, rows = _result(tmp_path, monkeypatch, [_spec('GoldPrice', 'complement', 'global_date_range'),
                                            _spec('InterestRate', 'complement', 'per_id_date_range')])
    assert rows['GoldPrice']['required_requests'] == 1
    assert rows['GoldPrice']['unbatched_requests'] == 4
    assert rows['InterestRate']['required_requests'] == 1
    assert rows['InterestRate']['blocked_tasks'] == 1
    assert rows['InterestRate']['state'] == 'partially_blocked'


def test_inflight_is_work_but_not_another_future_quota_call(tmp_path, monkeypatch):
    _database(tmp_path, 'sponsor', [('Price', '', '2020-01-01', 'day', 1, 'inflight', None)])
    _, rows = _result(tmp_path, monkeypatch, [_spec('Price')])
    assert rows['Price']['required_requests'] == 0
    assert rows['Price']['inflight_requests'] == 1


def test_missing_queue_unknown_is_not_zero(tmp_path, monkeypatch):
    result, rows = _result(tmp_path, monkeypatch, [_spec('Price')])
    assert rows['Price']['required_requests'] is None
    assert result['state'] == 'partial'
    assert result['summary']['count_basis'] == 'known_partial_subtotal'


def test_finite_priority_override_is_a_subset_not_an_extra_dataset(tmp_path, monkeypatch):
    dataset = 'TaiwanFuturesKBar'
    path = _database(tmp_path, 'complement', [
        (dataset, 'TX', '2026-09-21', 'id_day', 8, 'pending', None),
        (dataset, 'TX', '2026-09-22', 'id_day', 8, 'failed', (NOW + timedelta(hours=1)).isoformat()),
        (dataset, 'TX', '2026-09-23', 'id_day', 8, 'complete', None),
        (dataset, 'TX', '2026-09-24', 'id_day', 8, 'not_entitled', None),
        (dataset, 'TX', '2026-09-25', 'id_day', 8, 'inflight', None),
        (dataset, 'MTX', '2026-09-21', 'id_day', 8, 'pending', None),
    ])
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE finmind_priority_tasks(dataset TEXT,data_id TEXT,partition TEXT,PRIMARY KEY(dataset,data_id,partition))')
        conn.execute("INSERT INTO finmind_priority_tasks SELECT dataset,data_id,partition FROM tasks WHERE data_id='TX'")
    result, rows = _result(tmp_path, monkeypatch, [_spec(dataset, 'complement', 'per_futures_day')])
    priority = rows[dataset]['priority_override']
    assert priority == {'requests': 2, 'inflight_tasks': 1, 'blocked_tasks': 1,
                        'unsupported_tasks': 0, 'max_retry_wait_seconds': 3600, 'retry_tasks': 1}
    assert result['summary']['current_plan_requests'] == 3
    assert result['state'] == 'observed'


def test_legacy_day_priority_uses_worker_contract_and_incremental_is_not_counted_twice(tmp_path, monkeypatch):
    from downloader.finmind_eta_stages import stage_workloads

    dataset = 'TaiwanFuturesKBar'
    path = _database(tmp_path, 'complement', [
        (dataset, 'CR1', '2012-08-22', 'day', 8, 'pending', None),
        (dataset, 'CJ1', '2015-08-13', 'day', 8, 'failed', NOW.isoformat()),
        (dataset, 'TX', '2026-09-26', 'id_day', 0, 'pending', None),
    ])
    with sqlite3.connect(path) as connection:
        connection.execute('CREATE TABLE finmind_priority_tasks(dataset TEXT,data_id TEXT,partition TEXT)')
        connection.execute('INSERT INTO finmind_priority_tasks SELECT dataset,data_id,partition FROM tasks')
    result, rows = _result(tmp_path, monkeypatch, [_spec(dataset, 'complement', 'per_futures_day')])
    override = rows[dataset]['priority_override']
    assert override['requests'] == 2
    assert override['unsupported_tasks'] == 0
    assert rows[dataset]['incremental_requests'] == 1
    stages = stage_workloads(result)
    assert stages[0]['summary']['current_plan_requests'] == 3
    assert sum(item['summary']['current_plan_requests'] for item in stages) == 3
    assert all(item['summary']['current_plan_requests'] == 0 for item in stages[1:])


def test_unknown_priority_query_cannot_be_assumed_one_per_day(tmp_path, monkeypatch):
    from downloader.finmind_eta_stages import stage_workloads

    dataset = 'TaiwanFuturesKBar'
    path = _database(tmp_path, 'complement', [(dataset, '', '2012-08-22', 'day', 8, 'pending', None)])
    with sqlite3.connect(path) as connection:
        connection.execute('CREATE TABLE finmind_priority_tasks(dataset TEXT,data_id TEXT,partition TEXT)')
        connection.execute('INSERT INTO finmind_priority_tasks SELECT dataset,data_id,partition FROM tasks')
    result, rows = _result(tmp_path, monkeypatch, [_spec(dataset, 'complement', 'per_futures_day')])
    assert rows[dataset]['priority_override']['unsupported_tasks'] == 1
    with pytest.raises(ValueError, match='query_grain_unverified'):
        stage_workloads(result)


def test_unreadable_queue_unknown_and_no_database_created(tmp_path, monkeypatch):
    path = tmp_path / 'sponsor' / 'queue.sqlite3'
    path.parent.mkdir()
    path.write_bytes(b'not a sqlite file')
    result, rows = _result(tmp_path, monkeypatch, [_spec('Price')])
    assert rows['Price']['required_requests'] is None
    assert result['sources']['sponsor']['state'] == 'queue_unreadable'
    assert path.read_bytes() == b'not a sqlite file'
    assert not (tmp_path / 'complement').exists()


def test_snapshot_scope_exposes_not_scheduled_without_counting_zero_complete(tmp_path, monkeypatch):
    result, rows = _result(tmp_path, monkeypatch, [_spec('Tick', 'unscheduled', 'unscheduled_per_id_day'),
                                                _spec('News', 'disabled_policy', 'disabled_news')])
    assert rows['Tick']['current_plan_requests'] is None
    assert result['summary']['unscheduled_datasets'] == 1
    assert result['summary']['disabled_datasets'] == 1


def _free_fixture(tmp_path, observed=NOW):
    from downloader.download_finmind_free import SESSION_DATASETS, CALENDAR_DATASET, MASTER_DATASET
    status = {'observed_at_utc': observed.isoformat(), 'series': {
        dataset: {'total': 10, 'complete': 8, 'deferred': 1} for dataset in SESSION_DATASETS}}
    (tmp_path / 'status.json').write_text(json.dumps(status))
    (tmp_path / 'calendar.json').write_text(json.dumps({'source_dataset': CALENDAR_DATASET,
        'observed_at_utc': NOW.isoformat(), 'dates': ['2026-09-24']}))
    directory = tmp_path / 'receipts' / MASTER_DATASET
    directory.mkdir(parents=True)
    (directory / '2026-09-26.json').write_text(json.dumps({'dataset': MASTER_DATASET, 'status': 'complete'}))
    return [_spec(dataset, 'free', 'whole_market_day') for dataset in (*SESSION_DATASETS, CALENDAR_DATASET, MASTER_DATASET)]


def test_free_fresh_status_includes_deferred_once(tmp_path, monkeypatch):
    specs = _free_fixture(tmp_path)
    result, rows = _result(tmp_path, monkeypatch, specs)
    assert result['summary']['required_requests'] == 4
    assert result['summary']['cooling_tasks'] == 2
    assert result['sources']['free']['state'] == 'observed'


def test_free_eta_uses_worker_retry_heads_not_a_zero_second_guess(tmp_path, monkeypatch):
    specs = _free_fixture(tmp_path)
    path = tmp_path / 'status.json'
    payload = json.loads(path.read_text())
    for item in payload['series'].values():
        item.update(retry_tasks=1, earliest_retry_at_utc=(NOW + timedelta(minutes=1)).isoformat(),
                    latest_retry_at_utc=(NOW + timedelta(minutes=15)).isoformat())
    path.write_text(json.dumps(payload))
    result, rows = _result(tmp_path, monkeypatch, specs)
    assert result['summary']['retry_tasks'] == 2
    assert result['summary']['max_retry_wait_seconds'] == 900
    assert result['summary']['independent_retry_tasks'] == 2
    assert result['summary']['independent_retry_requests'] == 2
    assert all(row['independent_retry_clock_known'] for row in rows.values() if row['retry_tasks'])


def test_stale_free_status_is_unknown_not_finished(tmp_path, monkeypatch):
    specs = _free_fixture(tmp_path, NOW - timedelta(hours=2))
    result, rows = _result(tmp_path, monkeypatch, specs)
    assert result['summary']['unknown_datasets'] == 4
    assert all(row['required_requests'] is None for row in rows.values())


def test_naive_time_rejected(tmp_path):
    with pytest.raises(ValueError, match='timezone-aware'):
        work.build_finmind_workload(tmp_path, NOW.replace(tzinfo=None))


def test_workload_never_mutates_queue_or_uses_network(tmp_path, monkeypatch):
    import requests
    def forbidden(*args, **kwargs):
        raise AssertionError('workload inventory must not call HTTP')
    monkeypatch.setattr(requests.Session, 'request', forbidden)
    path = _database(tmp_path, 'sponsor', [('Price', '', '2020-01-01', 'day', 1, 'pending', None)])
    before = path.read_bytes()
    result, rows = _result(tmp_path, monkeypatch, [_spec('Price')])
    assert result['api_requests'] == result['queue_writes'] == result['parquet_scans'] == 0
    assert path.read_bytes() == before
    with sqlite3.connect(path) as conn:
        assert conn.execute('SELECT state FROM tasks').fetchone()[0] == 'pending'


def test_invalid_retry_timestamp_is_unknown_not_ready(tmp_path, monkeypatch):
    _database(tmp_path, 'sponsor', [('Price', '', '2020-01-01', 'day', 1, 'pending', 'not-a-date')])
    result, rows = _result(tmp_path, monkeypatch, [_spec('Price')])
    assert rows['Price']['required_requests'] is None
    assert result['sources']['sponsor']['state'] == 'queue_unreadable'


def test_due_failed_sponsor_neighbors_join_one_call(tmp_path, monkeypatch):
    dataset = 'TaiwanBusinessIndicator'
    _database(tmp_path, 'sponsor', [
        (dataset, '', f'{year}-01-01', 'year', 2, 'failed', NOW.isoformat()) for year in range(2010, 2013)
    ])
    _, rows = _result(tmp_path, monkeypatch, [_spec(dataset, shape='whole_market_date_range')])
    assert rows[dataset]['current_plan_requests'] == 1


@pytest.mark.parametrize('next_attempt', [None, 'invalid-timestamp'])
def test_queue_connection_closed_on_success_and_failure(tmp_path, monkeypatch, next_attempt):
    _database(tmp_path, 'sponsor', [('Price', '', '2020-01-01', 'day', 1, 'pending', next_attempt)])
    connect = sqlite3.connect
    connections = []

    def tracked_connect(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connections.append(connection)
        return connection

    monkeypatch.setattr(work.sqlite3, 'connect', tracked_connect)
    _result(tmp_path, monkeypatch, [_spec('Price')])
    assert len(connections) == 1
    with pytest.raises(sqlite3.ProgrammingError, match='closed'):
        connections[0].execute('SELECT 1')
