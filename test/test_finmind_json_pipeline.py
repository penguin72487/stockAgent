from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
import json
import threading
import time

import pyarrow.parquet as pq
import pytest

from downloader import download_finmind_complement as worker
from downloader import finmind_json_pipeline as pipeline
from downloader.common import SharedRateLimiter, RateLimitWaitCancelled

NOW = datetime(2026, 10, 7, 4, tzinfo=UTC)
US = 'USStockPriceMinute'


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    root = tmp_path / 'complement'
    root.mkdir()
    monkeypatch.setattr(worker, '_now', lambda: NOW)
    monkeypatch.setattr('downloader.finmind_scheduling.protected_stock_opening', lambda *a: False)
    monkeypatch.setattr('downloader.finmind_scheduling.incremental_reservation', lambda *a: {
        'ready_requests': 0, 'reserve_requests': 0, 'queue_errors': [], 'observed_at_utc': NOW.isoformat()})
    with worker._db(root / 'queue.sqlite3') as conn:
        pipeline.recover_claims(conn)
        worker._add_tasks(conn, [(US, code, '2024-06-03', 'id_day', 8) for code in ('AAPL', 'MSFT', 'NVDA', 'SPY')])
        conn.commit()
        yield root, conn


def rows(task):
    return [{'date': task.partition + ' 13:31:00', 'stock_id': task.data_id,
             'open': 100.123456, 'high': 101.23, 'low': 99.87, 'close': 100.99, 'volume': 1789}]


def run(runtime, **changes):
    root, conn = runtime
    options = {'config': {**pipeline.settings(), 'download_workers': 2}, 'max_calls': 4,
               'heartbeat': lambda *a: None}
    options.update(changes)
    account = {'tier': 'SponsorPro', 'observed_at_utc': NOW.isoformat(),
               'official_requests_per_hour': 20000, 'provider_used_in_hour': 0}
    with pipeline.Sessions() as sessions, ThreadPoolExecutor(2) as executor:
        return pipeline.run_batch(conn, executor, sessions, root, worker._next_task(conn, NOW),
            account, {'allowed': True, 'remaining': 20000, 'reserve': 2}, {'ready_requests': 0}, NOW,
            SharedRateLimiter(0, name='test-finmind-json'), 'not-a-token', **options)


def test_single_writer_parallel_and_exact_source_values(runtime, monkeypatch):
    root, conn = runtime
    barrier = threading.Barrier(2)
    calls = []
    owner = threading.get_ident()
    original = worker._store
    def store(*a, **kw):
        assert threading.get_ident() == owner
        return original(*a, **kw)
    def fetch(sessions, limiter, root, task, token, today):
        assert threading.get_ident() != owner
        calls.append((task.data_id, task.partition))
        barrier.wait(timeout=5)
        return rows(task), {'request_count': 1}
    monkeypatch.setattr(worker, '_store', store)
    monkeypatch.setattr(pipeline, '_fetch', fetch)
    result = run(runtime)
    assert result['calls'] == result['accepted_requests'] == result['nonempty_requests'] == 4
    assert len(set(calls)) == 4
    assert conn.execute('SELECT count(*) FROM finmind_object_prefetch_claims').fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM tasks WHERE state='complete'").fetchone()[0] == 4
    assert conn.execute('SELECT requests FROM finmind_json_pipeline_samples').fetchone()[0] == 4
    for code, day in calls:
        path = conn.execute('SELECT receipt_path FROM tasks WHERE data_id=?', (code,)).fetchone()[0]
        receipt = json.loads((root / path).read_bytes())
        assert pq.read_table(root / receipt['parquet_path']).to_pylist() == rows(worker.Task(US, code, day, 'id_day', 8, 'pending'))
        assert receipt['request']['json_pipeline_contract_version'] == pipeline.CONTRACT_VERSION


def test_complete_empty_and_exhausted_not_reopened(runtime, monkeypatch):
    root, conn = runtime
    conn.execute("UPDATE tasks SET state='retry_exhausted' WHERE data_id='SPY'")
    conn.execute("UPDATE tasks SET state='complete',next_attempt_at_utc=? WHERE data_id='NVDA'", ((NOW+timedelta(days=90)).isoformat(),))
    monkeypatch.setattr(pipeline, '_fetch', lambda sessions, limiter, root, task, *a: ([], {}))
    result = run(runtime)
    assert result['accepted_requests'] == 2 and result['nonempty_requests'] == 0
    assert dict(conn.execute('SELECT data_id,state FROM tasks')) == {
        'AAPL': 'observed_empty', 'MSFT': 'observed_empty', 'NVDA': 'complete', 'SPY': 'retry_exhausted'}


def test_wrong_identity_source_fails_without_touching_other_accepted_key(runtime, monkeypatch):
    def fetch(sessions, limiter, root, task, *a):
        value = rows(task)
        if task.data_id == 'AAPL':
            value[0]['stock_id'] = 'not-the-requested-symbol'
        worker.supplemental.validate_response(US, task.data_id, task.partition, NOW.date(), value)
        return value, {}
    monkeypatch.setattr(pipeline, '_fetch', fetch)
    result = run(runtime)
    assert result['failed_tasks'] == 1 and result['accepted_requests'] == 3
    assert runtime[1].execute("SELECT count(*) FROM finmind_json_pipeline_samples").fetchone()[0] == 0
    assert runtime[1].execute("SELECT state FROM tasks WHERE data_id='AAPL'").fetchone()[0] == 'failed'
    assert runtime[1].execute("SELECT error_code FROM tasks WHERE data_id='AAPL'").fetchone()[0] == 'supplemental_wrong_data_id'


def test_shared_request_contract_and_body_bound(runtime, monkeypatch):
    root, conn = runtime
    captured = []
    def fetch(session, limiter, ledger, dataset, token, params, **kw):
        captured.append((ledger, params, kw))
        return rows(worker.Task(dataset, params['data_id'], params['start_date'], 'id_day', 8, 'pending'))
    monkeypatch.setattr(worker, '_fetch_rows', fetch)
    task = worker._next_task(conn, NOW)
    serial = worker._request_independent(object(), object(), root, task, 'x', today=NOW.date())
    with pipeline.Sessions() as sessions:
        parallel = pipeline._fetch(sessions, object(), root, task, 'x', NOW.date())
    assert serial == parallel
    assert len(captured) == 2 and captured[0] == captured[1]
    assert captured[0][0] == root.parent
    assert captured[0][1] == {'dataset': US, 'data_id': task.data_id, 'start_date': task.partition}
    assert captured[0][2]['max_response_bytes'] == worker.BULK_MAX_RESPONSE_BYTES


def test_earlier_stage_blocks_us_and_exact_scope_cannot_expand(runtime, monkeypatch):
    root, conn = runtime
    worker._add_tasks(conn, [('TaiwanStockKBar', '2330', '2024-06-03', 'id_day', 8)])
    assert worker._next_task(conn, NOW).dataset == 'TaiwanStockKBar'
    monkeypatch.setattr(pipeline, '_fetch', lambda *a: pytest.fail('exact-key must retain serial path'))
    assert run(runtime, required_keys=((US, 'AAPL', '2024-06-03'),)) is None


def test_due_calendar_or_disk_guard_do_not_issue_new_calls(runtime, monkeypatch):
    monkeypatch.setattr(pipeline, '_fetch', lambda *a: pytest.fail('blocked admission'))
    monkeypatch.setattr('downloader.finmind_scheduling.incremental_reservation', lambda *a: {
        'ready_requests': 3, 'reserve_requests': 3, 'queue_errors': [], 'observed_at_utc': NOW.isoformat()})
    result = run(runtime)
    assert result['calls'] == 0 and result['state'] == 'incremental_reserve'


def test_safe_due_queries_parallelize_without_waiting_for_themselves(runtime, monkeypatch):
    runtime[1].execute('UPDATE tasks SET priority=0')
    monkeypatch.setattr('downloader.finmind_scheduling.incremental_reservation', lambda *a: {
        'ready_requests': 4, 'reserve_requests': 4, 'queue_errors': [], 'observed_at_utc': NOW.isoformat()})
    monkeypatch.setattr(pipeline, '_fetch', lambda sessions, limiter, root, task, *a: (rows(task), {}))
    assert run(runtime)['accepted_requests'] == 4


def test_continuous_refill_does_not_fall_back_to_redundant_old_maintenance(runtime, monkeypatch):
    root, conn = runtime
    monkeypatch.setattr(worker.supplemental, 'SOURCES', {US: worker.supplemental.SOURCES[US]})
    worker.supplemental.seed(conn, {'us': ['AAPL']}, NOW, include_status=False)
    worker._add_tasks(conn, [('TaiwanAssetSwapOptionDaily', 'OLD-BOND', 'history', 'id_history', 2)])
    conn.execute("UPDATE tasks SET state='complete',next_attempt_at_utc=? WHERE data_id='OLD-BOND'", (NOW.isoformat(),))
    calls = []
    def fetch(sessions, limiter, root, task, *a):
        calls.append((task.dataset, task.data_id, task.partition))
        return rows(task), {}
    monkeypatch.setattr(pipeline, '_fetch', fetch)
    result = run(runtime, max_calls=240, config={**pipeline.settings(), 'max_batch_requests': 240})
    assert result['accepted_requests'] == 240
    assert len(set(calls)) == 240 and {item[0] for item in calls} == {US}
    assert conn.execute("SELECT COUNT(*) FROM tasks WHERE dataset=? AND state IN ('pending','failed','inflight')", (US,)).fetchone()[0] <= worker.supplemental.WORKING_SET * 2
    assert conn.execute("SELECT state FROM tasks WHERE data_id='OLD-BOND'").fetchone()[0] == 'complete'


def test_budget_reserves_actual_inflight_and_other_lanes(runtime, monkeypatch):
    monkeypatch.setattr('downloader.finmind_account.backfill_budget', lambda *a, **kw: {
        'allowed': kw['in_flight'] < 1, 'remaining': 3, 'reserve': 2})
    monkeypatch.setattr(pipeline, '_fetch', lambda sessions, limiter, root, task, *a: (rows(task), {}))
    assert run(runtime, max_calls=1)['calls'] == 1


def test_capacity_requires_two_fresh_matching_full_batches(runtime):
    _, conn = runtime
    for seconds, fingerprint, age in ((20, pipeline.settings()['configuration_sha256'], 0),
                                     (40, pipeline.settings()['configuration_sha256'], 0),
                                     (.01, 'different-configuration', 0),
                                     (.01, pipeline.settings()['configuration_sha256'], 25)):
        conn.execute('INSERT INTO finmind_json_pipeline_samples '
                     '(dataset,requests,seconds,rows,nonempty_requests,configuration_sha256,observed_at_utc,cycle_wall_verified) VALUES (?,?,?,?,?,?,?,1)',
                     (US, 100, seconds, 39100, 100, fingerprint, (NOW-timedelta(hours=age)).isoformat()))
    result = pipeline.measured_rates(conn, US, NOW)
    assert result['samples'] == 2 and result['sampled_requests'] == 200
    assert result['rates']['central'] == pytest.approx(200*3600/60)
    assert result['rates']['slowest'] <= result['rates']['central'] <= result['rates']['fastest']


def test_measurement_includes_common_cycle_cost_and_only_excludes_measured_preemption(runtime, monkeypatch):
    _, conn = runtime
    config = pipeline.settings()
    monkeypatch.setattr(pipeline.time, 'monotonic', lambda: 200.)
    with pipeline.CycleCosts(conn, 100.):
        for _ in range(2):
            conn.execute('INSERT INTO finmind_json_pipeline_samples '
                         '(dataset,requests,seconds,rows,nonempty_requests,configuration_sha256,observed_at_utc) VALUES (?,?,?,?,?,?,?)',
                         (US, 100, 20, 39100, 100, config['configuration_sha256'], NOW.isoformat()))
    measured = pipeline.measured_rates(conn, US, NOW)
    assert measured['sampled_wall_seconds'] == 100
    assert measured['rates']['central'] == 200*3600/100
    with pipeline.CycleCosts(conn, 100.) as cycle:
        cycle.exclude_elapsed(120.)  # 80s of measured other-stage work.
        conn.execute('INSERT INTO finmind_json_pipeline_samples '
                     '(dataset,requests,seconds,rows,nonempty_requests,configuration_sha256,observed_at_utc) VALUES (?,?,?,?,?,?,?)',
                     (US, 100, 10, 39100, 100, config['configuration_sha256'], NOW.isoformat()))
    assert pipeline.measured_rates(conn, US, NOW)['sampled_wall_seconds'] == 120
    assert conn.execute('SELECT cycle_wall_seconds,excluded_work_seconds FROM finmind_json_pipeline_samples ORDER BY id DESC').fetchone() == (100, 80)
    with pipeline.CycleCosts(conn, 100.) as cycle:
        cycle.calibration_invalid = True
        conn.execute('INSERT INTO finmind_json_pipeline_samples '
                     '(dataset,requests,seconds,rows,nonempty_requests,configuration_sha256,observed_at_utc) VALUES (?,?,?,?,?,?,?)',
                     (US, 100, .01, 39100, 100, config['configuration_sha256'], NOW.isoformat()))
    assert pipeline.measured_rates(conn, US, NOW)['samples'] == 3


def test_cycle_quantum_is_part_of_capacity_fingerprint():
    production = pipeline.settings()
    assert production['max_requests_per_cycle'] == 1200
    assert production['max_batch_requests'] == production['max_requests_per_cycle']
    assert pipeline.settings(max_requests_per_cycle=120)['configuration_sha256'] != production['configuration_sha256']
    assert pipeline.settings(max_requests_per_cycle=1200) == production


def test_schema_migrates_old_measurements_without_touching_them(runtime):
    _, conn = runtime
    conn.execute('DROP TABLE finmind_json_pipeline_samples')
    conn.execute('CREATE TABLE finmind_json_pipeline_samples (id INTEGER PRIMARY KEY,dataset TEXT,requests INTEGER,seconds REAL,rows INTEGER,'
                 'nonempty_requests INTEGER,configuration_sha256 TEXT,observed_at_utc TEXT,fetch_validate_publish_seconds REAL,'
                 'cycle_wall_verified INTEGER DEFAULT 0)')
    assert pipeline.measured_rates(conn, US, NOW) is None
    pipeline._schema(conn)
    pipeline._schema(conn)
    assert 'cycle_started_at_utc' in {row[1] for row in conn.execute('PRAGMA table_info(finmind_json_pipeline_samples)')}


@pytest.mark.parametrize('key,value', [('download_workers', True), ('download_workers', 33),
                                     ('max_batch_requests', 0), ('max_batch_requests', 2401),
                                     ('max_buffered_response_bytes', 100)])
def test_config_invalid(tmp_path, key, value):
    path = tmp_path / 'config.json'
    path.write_text(json.dumps({**pipeline.settings(), key: value}))
    with pytest.raises(ValueError):
        pipeline.settings(path)


def test_cancelled_rate_wait_does_not_block_pool_shutdown(tmp_path):
    limiter = SharedRateLimiter(.18, name='test-json-cancellation', state_dir=tmp_path)
    limiter.defer(3600)
    stop = threading.Event()
    with ThreadPoolExecutor(1) as executor:
        future = executor.submit(limiter.wait, cancel_event=stop)
        deadline = time.monotonic() + 2
        while not limiter.pending_waiters() and time.monotonic() < deadline:
            time.sleep(.01)
        stop.set()
        with pytest.raises(RateLimitWaitCancelled):
            future.result(timeout=1)
    assert limiter.pending_waiters() == 0


def test_loop_cycles_reuse_one_limiter_and_verified_tier_changes(monkeypatch):
    from downloader import finmind_account
    monkeypatch.setattr(finmind_account, '_LIMITER', None)
    objects = []
    def create(interval, **kw):
        value = type('Limiter', (), {'interval_seconds': interval, 'name': kw['name']})()
        objects.append(value)
        return value
    monkeypatch.setattr(finmind_account, 'SharedRateLimiter', create)
    for _ in range(1000):
        limiter = finmind_account.rate_limiter({'official_requests_per_hour': 20000})
    assert objects == [limiter]
    assert limiter.interval_seconds == .18 and limiter.name == 'finmind-v4-data'
    assert finmind_account.rate_limiter({'official_requests_per_hour': 600}) is limiter
    assert limiter.interval_seconds == 6
