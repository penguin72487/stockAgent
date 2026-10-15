import copy
from datetime import UTC, datetime
import json
import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import sha256_file
from scripts.verify_finmind_opening_resume import completed_readback, resume_window, stamp


@pytest.fixture
def observation():
    return {'state': 'observed', 'native_owner_unchanged': True,
            'sampled_prior_source_files_unchanged': True, 'observer_provider_calls': 0,
            'observer_queue_writes': 0, 'service_controls': 0,
            'samples': [
                {'phase': 'waiting', 'reason': 'protected_opening',
                 'observed_at_utc': '2026-10-15T00:59:00Z', 'next_check_at_utc': '2026-10-15T01:10:00Z'},
                {'phase': 'running', 'observed_at_utc': '2026-10-15T01:10:01Z',
                 'heartbeat_at_utc': '2026-10-15T01:10:00.013Z'},
                {'phase': 'running', 'observed_at_utc': '2026-10-15T01:11:03Z',
                 'last_task': {'dataset': 'UKStockPrice', 'data_id': 'TEST.L', 'partition': 'history',
                               'status': 'complete', 'rows': 2, 'observed_at_utc': '2026-10-15T01:11:01Z'}}]}


def test_heartbeat_and_actual_completed_clocks_remain_separate(observation):
    deadline, wake, completed = resume_window(observation)
    assert (wake - deadline).total_seconds() == .013
    assert completed[0]['observed_at_utc'] == '2026-10-15T01:11:01Z'
    assert stamp('2026-10-15T09:10:00+08:00') == deadline
    with pytest.raises(ValueError, match='timezone'):
        stamp('2026-10-15T09:10:00')


@pytest.mark.parametrize('change', ['owner', 'prior_source', 'network', 'old_receipt', 'heartbeat_only', 'late_wake'])
def test_incomplete_or_mutating_resume_proofs_are_rejected(observation, change):
    bad = copy.deepcopy(observation)
    if change == 'owner':
        bad['native_owner_unchanged'] = False
    elif change == 'prior_source':
        bad['sampled_prior_source_files_unchanged'] = False
    elif change == 'network':
        bad['observer_provider_calls'] = 1
    elif change == 'old_receipt':
        bad['samples'][-1]['last_task']['observed_at_utc'] = '2026-10-15T01:09:00Z'
    elif change == 'late_wake':
        bad['samples'][1]['heartbeat_at_utc'] = '2026-10-15T01:10:06Z'
    else:
        del bad['samples'][-1]['last_task']
    with pytest.raises(ValueError):
        resume_window(bad)


def test_completed_readback_requires_current_rows_full_hash_and_new_fetch(tmp_path):
    parquet = tmp_path / 'body.parquet'
    pq.write_table(pa.table({'date': ['2026-10-13', '2026-10-14'], 'close': [11., 12.]}), parquet)
    observed = {'dataset': 'UKStockPrice', 'data_id': 'TEST.L', 'partition': 'history', 'rows': 2}
    receipt = {**observed, 'status': 'complete', 'fetched_at_utc': '2026-10-15T01:11:00Z',
               'parquet_path': 'body.parquet', 'parquet_size_bytes': parquet.stat().st_size,
               'sha256': sha256_file(parquet), 'source_first_date': '2026-10-13',
               'source_last_date': '2026-10-14', 'coverage_claim': 'observed_response_only_not_provider_completeness'}
    path = tmp_path / 'receipt.json'
    path.write_text(json.dumps(receipt))
    with sqlite3.connect(':memory:') as connection:
        connection.execute('CREATE TABLE tasks (dataset, data_id, partition, state, rows, receipt_path)')
        connection.execute('INSERT INTO tasks VALUES (?,?,?,?,?,?)',
                           ('UKStockPrice', 'TEST.L', 'history', 'complete', 2, 'receipt.json'))
        deadline = datetime(2026, 10, 15, 1, 10, tzinfo=UTC)
        result = completed_readback(connection, tmp_path, observed, deadline)
        assert result['full_parquet_hash_and_footer_verified'] and result['rows'] == 2
        receipt['fetched_at_utc'] = '2026-10-15T01:09:00Z'
        path.write_text(json.dumps(receipt))
        with pytest.raises(ValueError, match='old receipt'):
            completed_readback(connection, tmp_path, observed, deadline)
        receipt['fetched_at_utc'] = '2026-10-15T01:11:00Z'
        receipt['sha256'] = '0' * 64
        path.write_text(json.dumps(receipt))
        with pytest.raises(ValueError, match='parquet_hash_mismatch'):
            completed_readback(connection, tmp_path, observed, deadline)
