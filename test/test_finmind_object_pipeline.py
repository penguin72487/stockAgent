from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import json
import threading

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader import download_finmind_complement as worker
from downloader import finmind_object_pipeline as pipeline
from downloader import finmind_storage_objects as objects

NOW = datetime(2026, 10, 6, 16, tzinfo=UTC)
DATASET = pipeline.DATASET


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    monkeypatch.setattr('downloader.finmind_scheduling.incremental_reservation', lambda *a: {'ready_requests': 0})
    monkeypatch.setattr('downloader.finmind_scheduling.protected_stock_opening', lambda *a: False)
    root = tmp_path / 'complement'
    root.mkdir()
    with worker._db(root / 'queue.sqlite3') as connection:
        objects.configure(connection, {'tier': 'SponsorPro'}, NOW)
        pipeline.recover_claims(connection)
        worker._add_tasks(connection, [(DATASET, '', f'2024-06-{day}', 'id_day', 8) for day in (20, 21, 24, 25)])
        worker._add_tasks(connection, [('TaiwanStockWarrantTradingDailyReport', '', '2024-06-25', 'id_day', 8)])
        connection.commit()
        owner_thread = threading.get_ident()
        original_store = worker._store
        def store(*args, **kwargs):
            assert threading.get_ident() == owner_thread
            return original_store(*args, **kwargs)
        monkeypatch.setattr(worker, '_store', store)
        yield root, connection


def source(root, task):
    rows = [{'date': task.partition, 'stock_id': '2330', 'securities_trader_id': '1020',
             'securities_trader': 'broker', 'price': 101.01, 'buy': 17, 'sell': 3}]
    path = root / f'transfer-{task.partition}.parquet'
    pq.write_table(pa.Table.from_pylist(rows), path)
    checked = objects.validate_parquet(path, DATASET, task.partition)
    value = objects.StorageObject(path, 1, worker._sha256(path), checked['field_non_null_counts'], {})
    return value, {'endpoint': 'storage_objects', 'transfer_and_validation_seconds': .5}


def run(connection, root, *, config=None, **changes):
    options = {'config': config or {**pipeline.settings(), 'download_workers': 2, 'derivation_workers': 2},
               'max_calls': 2, 'heartbeat': lambda *a: None}
    options.update(changes)
    current = worker._next_task(connection, NOW)
    with ThreadPoolExecutor(2) as downloads, ThreadPoolExecutor(2) as processors:
        return pipeline.run_batch(connection, downloads, processors, root, current,
            {'tier': 'SponsorPro', 'observed_at_utc': NOW.isoformat()},
            {'allowed': True, 'remaining': 20000}, {'ready_requests': 0}, NOW,
            object(), 'never-a-live-token', **options)


def test_parallel_download_exact_derived_publication_one_writer_and_max_calls(runtime, monkeypatch):
    root, connection = runtime
    barrier = threading.Barrier(2)
    calls = []
    def fetch(limiter, root, task, token):
        calls.append(task.partition)
        barrier.wait(timeout=5)
        return source(root, task)
    monkeypatch.setattr(pipeline, '_fetch', fetch)
    result = run(connection, root)
    assert result['calls'] == result['accepted_requests'] == 2
    assert result['failed_tasks'] == 0
    assert len(set(calls)) == 2
    assert connection.execute("SELECT COUNT(*) FROM tasks WHERE dataset=? AND state='complete'", (DATASET,)).fetchone()[0] == 2
    assert connection.execute("SELECT COUNT(*) FROM tasks WHERE dataset=? AND state='complete'", (DATASET + 'SecIdAgg',)).fetchone()[0] == 2
    assert connection.execute("SELECT state FROM tasks WHERE dataset='TaiwanStockWarrantTradingDailyReport'").fetchone()[0] == 'pending'
    assert connection.execute('SELECT COUNT(*) FROM finmind_object_prefetch_claims').fetchone()[0] == 0
    assert connection.execute('SELECT requests FROM finmind_pipeline_samples').fetchone()[0] == 2
    for partition in calls:
        receipt = json.loads((root / 'receipts' / DATASET / 'all' / f'{partition}.json').read_bytes())
        assert receipt['source_bytes_preserved'] is True


@pytest.mark.parametrize('changes', [{'datasets': (DATASET,)}, {'required_keys': ((DATASET, '', '2024-06-25'),)}, {'max_calls': 0}])
def test_exact_key_scope_and_zero_budget_cannot_expand(runtime, monkeypatch, changes):
    root, connection = runtime
    monkeypatch.setattr(pipeline, '_fetch', lambda *a: pytest.fail('out-of-scope call'))
    assert run(connection, root, **changes) is None


def test_due_incremental_preempts_without_sending_new_calls(runtime, monkeypatch):
    root, connection = runtime
    monkeypatch.setattr('downloader.finmind_scheduling.incremental_reservation', lambda *a: {'ready_requests': 10})
    monkeypatch.setattr(pipeline, '_fetch', lambda *a: pytest.fail('new call while due refresh waits'))
    result = run(connection, root)
    assert result['calls'] == 0 and result['state'] == 'incremental_reserve'


def test_failed_source_does_not_reopen_an_accepted_other_partition(runtime, monkeypatch):
    root, connection = runtime
    def fetch(limiter, root, task, token):
        if task.partition == '2024-06-25':
            raise worker.SourceError('provider_server_error', retry_after=30)
        return source(root, task)
    monkeypatch.setattr(pipeline, '_fetch', fetch)
    result = run(connection, root)
    assert result['accepted_requests'] == result['failed_tasks'] == 1
    assert connection.execute('SELECT state FROM tasks WHERE dataset=? AND partition=?',
                              (DATASET, '2024-06-25')).fetchone()[0] == 'failed'
    assert connection.execute('SELECT state FROM tasks WHERE dataset=? AND partition=?',
                              (DATASET + 'SecIdAgg', '2024-06-24')).fetchone()[0] == 'complete'
    assert connection.execute('SELECT COUNT(*) FROM finmind_pipeline_samples').fetchone()[0] == 0


def test_restart_restores_only_claimed_inflight_not_successes_or_exhausted(runtime):
    root, connection = runtime
    task = worker._next_task(connection, NOW)
    pipeline._claim(connection, task)
    worker._add_tasks(connection, [('UKStockPrice', 'missing', 'history', 'id_history', 5)])
    connection.execute("UPDATE tasks SET state='retry_exhausted' WHERE dataset='UKStockPrice'")
    connection.commit()
    pipeline.recover_claims(connection)
    assert connection.execute('SELECT state FROM tasks WHERE dataset=? AND partition=?',
                              (task.dataset, task.partition)).fetchone()[0] == task.state
    assert connection.execute("SELECT state FROM tasks WHERE dataset='UKStockPrice'").fetchone()[0] == 'retry_exhausted'
    pipeline._claim(connection, task)
    connection.execute("UPDATE tasks SET state='complete' WHERE dataset=? AND partition=?", (task.dataset, task.partition))
    pipeline.recover_claims(connection)
    assert connection.execute('SELECT state FROM tasks WHERE dataset=? AND partition=?',
                              (task.dataset, task.partition)).fetchone()[0] == 'complete'


@pytest.mark.parametrize('field,value', [('download_workers', True), ('derivation_workers', 99), ('arrow_threads', 0), ('max_batch_requests', 33)])
def test_configuration_rejects_unbounded_or_boolean_counts(tmp_path, field, value):
    config = {**pipeline.settings(), field: value, 'schema_version': 1}
    path = tmp_path / 'config.json'
    path.write_text(json.dumps(config))
    with pytest.raises(ValueError, match='invalid_object_pipeline_configuration'):
        pipeline.settings(path)


def test_capacity_central_is_accepted_work_over_full_wall_not_fast_upper_median(runtime):
    _, connection = runtime
    config = pipeline.settings()
    # Same batch size; two slow startup/network waves and two fast waves.
    for calls, seconds in ((16, 116), (16, 170), (16, 37), (16, 34)):
        connection.execute('INSERT INTO finmind_pipeline_samples '
                           '(dataset,requests,seconds,rows,configuration_sha256,observed_at_utc) VALUES (?,?,?,?,?,?)',
                           (DATASET, calls, seconds, 2000000, config['configuration_sha256'], NOW.isoformat()))
    # A different configuration's impressive fixture cannot enter this rate.
    connection.execute('INSERT INTO finmind_pipeline_samples '
                       '(dataset,requests,seconds,rows,configuration_sha256,observed_at_utc) VALUES (?,?,?,?,?,?)',
                       (DATASET, 16, .1, 2000000, 'different-owner-profile', NOW.isoformat()))
    measured = pipeline.measured_rates(connection, DATASET)
    assert measured['samples'] == 4
    assert measured['sampled_requests'] == 64
    assert measured['sampled_wall_seconds'] == 357
    assert measured['rates']['central'] == pytest.approx(64*3600/357)
    assert measured['rates']['central'] < 16*3600/37
    assert measured['rates']['slowest'] <= measured['rates']['central'] <= measured['rates']['fastest']
