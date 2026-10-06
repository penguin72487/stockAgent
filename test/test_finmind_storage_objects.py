from datetime import UTC, date, datetime
import io
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader import download_finmind_complement as worker
from downloader import finmind_storage_objects as objects
from downloader import finmind_supplemental as supplemental
from downloader.finmind_account import pacing_interval, rate_limiter


NOW = datetime(2026, 10, 6, 10, tzinfo=UTC)
DATASET = 'TaiwanStockKBar'
DAY = '2026-10-05'


def kbars(day=DAY):
    return [{'date': day, 'stock_id': '2330', 'minute': '09:01:00',
             'open': 100., 'high': 101., 'low': 99., 'close': 100., 'volume': 5}]


def test_pro_paces_one_verified_account_at_20000_not_each_worker():
    limiter = rate_limiter({'official_requests_per_hour': 20000})
    assert limiter.name == 'finmind-v4-data'
    assert limiter.interval_seconds == 0.18
    assert pacing_interval({'official_requests_per_hour': 6000}) == 0.6
    with pytest.raises(ValueError):
        pacing_interval({'official_requests_per_hour': True})


def test_profile_migration_is_reversible_retains_artifacts_and_taiex(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [(DATASET, '2330', DAY, 'id_day', 8),
                                (DATASET, 'TAIEX', '2018-12-31', 'id_day', 8),
                                (DATASET, 'TAIEX', DAY, 'id_day', 8)])
        conn.execute("UPDATE tasks SET state='complete',rows=2,receipt_path='original.json' WHERE data_id='2330'")
        before = conn.execute("SELECT * FROM tasks WHERE data_id='2330'").fetchone()
        objects.configure(conn, {'tier': 'SponsorPro'}, NOW)
        assert conn.execute("SELECT state,rows,receipt_path FROM tasks WHERE data_id='2330'").fetchone() == (
            'deprecated_query_shape', 2, 'original.json')
        assert conn.execute("SELECT state FROM tasks WHERE data_id='TAIEX' AND partition='2018-12-31'").fetchone()[0] == 'pending'
        assert conn.execute("SELECT state FROM tasks WHERE data_id='TAIEX' AND partition=?", (DAY,)).fetchone()[0] == 'deprecated_query_shape'
        assert len(objects.active_datasets(conn)) == 7
        objects.configure(conn, {'tier': 'SponsorPro'}, NOW)
        assert conn.execute('SELECT count(*) FROM finmind_object_migrations').fetchone()[0] == 2
        objects.configure(conn, {'tier': 'Sponsor'}, NOW)
        assert not objects.active_datasets(conn)
        assert conn.execute("SELECT * FROM tasks WHERE data_id='2330'").fetchone() == before
        conn.execute("UPDATE tasks SET state='failed',next_attempt_at_utc='2026-10-07T00:00:00+00:00' "
                     "WHERE data_id='2330'")
        objects.configure(conn, {'tier': 'SponsorPro'}, NOW)
        objects.configure(conn, {'tier': 'Sponsor'}, NOW)
        assert conn.execute("SELECT state,next_attempt_at_utc FROM tasks WHERE data_id='2330'").fetchone() == (
            'failed', '2026-10-07T00:00:00+00:00')


def test_whole_market_frontier_does_not_multiply_by_old_ids_and_keeps_taiex(tmp_path, monkeypatch):
    # Short fixtures exercise the same source/TAIEX lower-bound logic.
    monkeypatch.setattr(supplemental, 'SOURCES', {DATASET: supplemental.Source(
        'stocks', date(2026, 10, 1), 'day', 8, 15)})
    monkeypatch.setitem(objects.OBJECT_FIRST, DATASET, date(2026, 10, 1))
    monkeypatch.setattr(supplemental, 'history_floor', lambda ds, identifier: date(2026, 10, 1))
    monkeypatch.setattr(supplemental, 'WORKING_SET', 1)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        supplemental.seed(conn, {'stocks': ['2330', '2317']}, NOW)
        objects.configure(conn, {'tier': 'SponsorPro'}, NOW)
        supplemental.seed(conn, {'stocks': ['2330', '2317']}, NOW)
        stats = supplemental.frontier_status(conn, NOW)[DATASET]
        assert stats['known_identifiers'] == 2  # one all-market object + TAIEX
        assert stats['query_shape'] == 'whole_market_storage_object_day'
        assert stats['object_unseeded_candidates'] <= stats['raw_unseeded_calendar_candidates']
        assert conn.execute("SELECT count(*) FROM finmind_source_frontiers WHERE data_id='2330'").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM tasks WHERE data_id='2330' AND state='pending'").fetchone()[0] == 0


def test_index_legacy_frontier_stops_before_object_floor_and_resumes_on_downgrade(tmp_path, monkeypatch):
    monkeypatch.setattr(supplemental, 'SOURCES', {DATASET: supplemental.Source(
        'stocks', date(2026, 10, 1), 'day', 8, 15)})
    monkeypatch.setitem(objects.OBJECT_FIRST, DATASET, date(2026, 10, 1))
    monkeypatch.setattr(supplemental, 'history_floor', lambda ds, identifier: date(2026, 9, 28))
    monkeypatch.setattr(supplemental, 'WORKING_SET', 1)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        supplemental.seed(conn, {'stocks': ['2330']}, NOW)
        prior = conn.execute("SELECT older_than FROM finmind_source_frontiers WHERE data_id='TAIEX'").fetchone()
        objects.configure(conn, {'tier': 'SponsorPro'}, NOW)
        assert conn.execute("SELECT older_than FROM finmind_source_frontiers WHERE data_id='TAIEX'").fetchone() == ('2026-09-30',)
        supplemental.seed(conn, {}, NOW)
        assert conn.execute("SELECT count(*) FROM tasks WHERE data_id='TAIEX' AND partition>='2026-10-01' "
                             "AND state='pending'").fetchone()[0] == 0
        objects.configure(conn, {'tier': 'Sponsor'}, NOW)
        assert conn.execute("SELECT older_than FROM finmind_source_frontiers WHERE data_id='TAIEX'").fetchone() == prior


def test_object_floor_is_not_the_older_legacy_floor(tmp_path, monkeypatch):
    monkeypatch.setattr(supplemental, 'SOURCES', {DATASET: supplemental.Source(
        'stocks', date(2026, 10, 1), 'day', 8, 15)})
    monkeypatch.setitem(objects.OBJECT_FIRST, DATASET, date(2026, 10, 2))
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        objects.configure(conn, {'tier': 'SponsorPro'}, NOW)
        supplemental.seed(conn, {}, NOW)
        assert conn.execute("SELECT older_than FROM finmind_source_frontiers WHERE dataset=? AND data_id=''",
                            (DATASET,)).fetchone() == (None,)
        assert conn.execute("SELECT min(partition) FROM tasks WHERE dataset=? AND data_id=''", (DATASET,)).fetchone() == (
            '2026-10-02',)
        assert supplemental.frontier_status(conn, NOW)[DATASET]['object_unseeded_candidates'] == 0


@pytest.mark.parametrize('mutation,error', [
    ({'date': '2026-10-04'}, 'object_response_outside_day'),
    ({'volume': -1}, 'object_invalid_numeric_value'),
    ({'close': float('nan')}, 'object_invalid_numeric_value'),
    ({'high': 90.}, 'object_invalid_ohlc'),
    ({'stock_id': ''}, 'object_empty_identity'),
])
def test_full_arrow_validation_rejects_invalid_source_rows(tmp_path, mutation, error):
    path = tmp_path / 'raw.parquet'
    rows = kbars() * 2
    rows[-1] = {**rows[-1], **mutation}
    pq.write_table(pa.Table.from_pylist(rows), path)
    with pytest.raises(ValueError, match=error):
        objects.validate_parquet(path, DATASET, DAY)


def test_parquet_keeps_native_bytes_duplicates_and_receipt_history(tmp_path):
    task = worker.Task(DATASET, '', DAY, 'id_day', 8, 'pending')
    def store(rows):
        path = tmp_path / 'temporary.parquet'
        pq.write_table(pa.Table.from_pylist(rows), path, compression='snappy')
        raw = path.read_bytes()
        checked = objects.validate_parquet(path, DATASET, DAY)
        value = objects.StorageObject(path, checked['rows'], worker._sha256(path), checked['field_non_null_counts'], {})
        receipt = worker._store(tmp_path, task, value, NOW, request_metadata={
            'endpoint': 'storage_objects', 'request_count': 1, 'response_rows': checked['rows'],
            'transfer_and_validation_seconds': 2})
        assert (tmp_path / receipt['parquet_path']).read_bytes() == raw
        return receipt
    first = store(kbars() * 2)
    assert first['rows'] == 2 and first['source_bytes_preserved']
    assert first['volume_units']['stock_share_unit_status'] == 'mixed_market_lots_or_shares_preserved'
    store(kbars())
    assert len(list((tmp_path / 'receipt_history').rglob('*.json'))) == 1
    assert (tmp_path / first['parquet_path']).is_file()


def test_derivative_signed_spread_prices_are_not_rejected_or_normalized(tmp_path):
    path = tmp_path / 'raw.parquet'
    row = {'date': DAY + ' 08:45:00', 'futures_id': 'TX',
           'contract_date': '202610/202611', 'price': -25, 'volume': 4}
    pq.write_table(pa.Table.from_pylist([row, row]), path)
    checked = objects.validate_parquet(path, 'TaiwanFuturesTick', DAY)
    assert checked['rows'] == 2
    assert pq.read_table(path)['price'].to_pylist() == [-25, -25]


@pytest.mark.parametrize('dataset', ['TaiwanFuturesTick', 'TaiwanOptionTick'])
def test_0600_release_uses_preceding_physical_date_and_does_not_cool_future_empty_90_days(dataset):
    source = supplemental.SOURCES[dataset]
    before = datetime(2026, 10, 5, 21, 59, tzinfo=UTC)  # Taipei Oct 6 05:59.
    after = datetime(2026, 10, 5, 22, tzinfo=UTC)
    assert supplemental._eligible_anchor(source, before) == date(2026, 10, 4)
    assert supplemental._eligible_anchor(source, after) == date(2026, 10, 5)
    task = worker.Task(dataset, '', '2026-10-06', 'id_day', 10, 'observed_empty')
    assert worker._next_refresh(task, NOW, empty=True) == '2026-10-06T22:00:00+00:00'


class Response:
    def __init__(self, status, body=b'', headers=None):
        self.status_code, self.body, self.headers = status, body, headers or {}
    def close(self):
        pass
    def iter_content(self, **kwargs):
        yield self.body


def test_signed_redirect_never_forwards_token_or_counts_transfer_as_api(tmp_path, monkeypatch):
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(kbars()), buffer)
    responses = iter([Response(307, headers={'Location': 'https://jp-tyo-1.linodeobjects.com/day?signature=private'}),
                      Response(200, buffer.getvalue())])
    calls, grants = [], []
    class Session:
        def get(self, url, **kwargs):
            calls.append((url, kwargs))
            return next(responses)
    class Limiter:
        def wait(self):
            grants.append('grant')
    monkeypatch.setattr('downloader.download_finmind_free._record_request_start', lambda *args: grants.append('ledger'))
    task = worker.Task(DATASET, '', DAY, 'id_day', 8, 'pending')
    value, metadata = objects.fetch(Session(), Limiter(), tmp_path, task, 'secret-token')
    assert grants == ['grant', 'ledger']
    assert calls[0][1]['headers']['Authorization'] == 'Bearer secret-token'
    assert not calls[1][1].get('headers', {}).get('Authorization')
    assert metadata['request_count'] == 1 and metadata['signed_transfer_requests'] == 1
    assert 'private' not in json.dumps(metadata)
    assert value.rows == 1
    value.path.unlink()


@pytest.mark.parametrize('url', ['http://jp-tyo-1.linodeobjects.com/raw', 'https://example.com/raw',
                                'https://jp-tyo-1.linodeobjects.com.evil.test/raw',
                                'https://user:pass@jp-tyo-1.linodeobjects.com/raw'])
def test_untrusted_redirects_rejected(url):
    with pytest.raises(ValueError):
        objects._safe_object_url(url)


def test_empty_object_is_not_complete_or_a_verified_holiday(tmp_path, monkeypatch):
    class Session:
        def get(self, *args, **kwargs):
            return Response(404)
    class Limiter:
        def wait(self):
            pass
    monkeypatch.setattr('downloader.download_finmind_free._record_request_start', lambda *args: None)
    task = worker.Task(DATASET, '', DAY, 'id_day', 8, 'pending')
    rows, metadata = objects.fetch(Session(), Limiter(), tmp_path, task, 'secret')
    receipt = worker._store(tmp_path, task, rows, NOW, request_metadata=metadata)
    assert receipt['status'] == 'observed_empty'
    assert 'not_verified_market_closure' in metadata['object_availability']


def test_broker_derivation_reads_whole_market_objects_without_per_id_validation(tmp_path):
    dataset = 'TaiwanStockTradingDailyReport'
    task = worker.Task(dataset, '', DAY, 'id_day', 8, 'pending')
    rows = [{'date': DAY, 'stock_id': stock, 'securities_trader_id': '1020',
             'securities_trader': 'broker', 'price': 10., 'buy': 3, 'sell': 1} for stock in ['2330', '2317']]
    path = tmp_path / 'temporary.parquet'
    pq.write_table(pa.Table.from_pylist(rows), path)
    checked = objects.validate_parquet(path, dataset, DAY)
    value = objects.StorageObject(path, 2, worker._sha256(path), checked['field_non_null_counts'], {})
    worker._store(tmp_path, task, value, NOW, request_metadata={
        'endpoint': 'storage_objects', 'transfer_and_validation_seconds': 2})
    derived, metadata = worker._derive_broker_aggregate(tmp_path, worker.Task(
        'TaiwanStockTradingDailyReportSecIdAgg', '', DAY, 'derived', 7, 'pending'))
    assert {row['stock_id'] for row in derived} == {'2330', '2317'}
    assert metadata['request_count'] == 0
    assert metadata['local_derivation_seconds'] > 0


def test_broker_transfer_cost_requires_and_includes_zero_call_aggregate(tmp_path):
    dataset = 'TaiwanStockTradingDailyReport'
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        objects.configure(conn, {'tier': 'SponsorPro'}, NOW)
        conn.execute('INSERT INTO finmind_object_transfer_samples VALUES (1,?,?,?,?,?,?)',
                     (dataset, DAY, 2., 100, 10, NOW.isoformat()))
        stats = objects.transfer_statistics(conn)[dataset]
        assert stats['dependent_processing_unknown'] is True
        conn.execute('INSERT INTO finmind_object_derivation_samples VALUES (1,?,?,?,?)',
                     (dataset, DAY, 20., NOW.isoformat()))
        stats = objects.transfer_statistics(conn)[dataset]
        assert stats['dependent_processing_unknown'] is False
        assert stats['median_seconds'] == 22.
        assert stats['dependent_processing_samples'] == 1
