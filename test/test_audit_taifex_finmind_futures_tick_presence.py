import json
import sqlite3
from datetime import date

import polars as pl

from downloader.artifact_io import sha256_file
from scripts.audit_taifex_finmind_futures_tick_presence import audit


def test_source_presence_deduplicates_and_keeps_unavailable_days_separate(tmp_path):
    official = tmp_path / 'official.parquet'
    pl.DataFrame({
        'date': [date(2020, 1, 2)] * 4 + [date(2020, 1, 3)],
        'product': ['TX', 'TX', 'MTX', 'TX', 'TX'],
        'contract': ['202001', '202001', '202001W1', '202001', '202001'],
        'series_type': ['monthly', 'monthly', 'weekly', 'monthly', 'monthly'],
        'session': ['一般', '一般', '一般', '盤後', '一般'],
        'session_reported': [True] * 5,
        'volume': [10, 10, 2, 30, 1],
    }).write_parquet(official)
    root = tmp_path / 'provider'
    receipt_dir = root / 'receipts/TaiwanFuturesTick/all'
    receipt_dir.mkdir(parents=True)
    payload = root / 'ticks.parquet'
    pl.DataFrame({'date': ['2020-01-02 01:00:00', '2020-01-02 09:00:00'],
                  'futures_id': ['TX', 'TX'], 'contract_date': ['202001', '202001'],
                  'volume': [3, 5]}).write_parquet(payload)
    receipt = {'dataset': 'TaiwanFuturesTick', 'data_id': '', 'partition': '2020-01-02',
               'status': 'complete', 'rows': 2, 'parquet_path': 'ticks.parquet',
               'parquet_size_bytes': payload.stat().st_size, 'sha256': sha256_file(payload)}
    (receipt_dir / '2020-01-02.json').write_text(json.dumps(receipt))
    (receipt_dir / '2020-01-03.json').write_text(json.dumps({
        'dataset': 'TaiwanFuturesTick', 'data_id': '', 'partition': '2020-01-03',
        'status': 'observed_empty', 'rows': 0,
    }))
    with sqlite3.connect(root / 'queue.sqlite3') as con:
        con.execute('CREATE TABLE tasks(dataset,data_id,partition,state,error_code)')
        con.executemany('INSERT INTO tasks VALUES (?,?,?,?,?)', [
            ('TaiwanFuturesTick', '', '2020-01-02', 'complete', None),
            ('TaiwanFuturesTick', '', '2020-01-03', 'observed_empty', None),
        ])
    summary, daily, unmatched = audit(official, sha256_file(official), root,
                                       start=date(2020, 1, 2), end=date(2020, 1, 3))
    assert summary['official_positive_contract_days'] == 3
    assert summary['matched_contract_days_on_natural_date'] == 1
    assert summary['unmatched_contract_days_on_verified_natural_dates'] == 1
    assert summary['official_contract_days_without_verified_provider_payload'] == 1
    assert daily[1]['status'] == 'receipt_not_complete'
    assert [(r['product'], r['contract']) for r in unmatched] == [('MTX', '202001W1')]


def test_bad_provider_hash_is_not_misreported_as_missing_contract(tmp_path):
    official = tmp_path / 'official.parquet'
    pl.DataFrame({'date': [date(2020, 1, 2)], 'product': ['TX'], 'contract': ['202001'],
                  'series_type': ['monthly'], 'session': ['一般'], 'session_reported': [True],
                  'volume': [1]}).write_parquet(official)
    root = tmp_path / 'provider'
    receipt_dir = root / 'receipts/TaiwanFuturesTick/all'
    receipt_dir.mkdir(parents=True)
    payload = root / 'ticks.parquet'
    pl.DataFrame({'date': ['2020-01-02 09:00:00'], 'futures_id': ['TX'],
                  'contract_date': ['202001'], 'volume': [1]}).write_parquet(payload)
    (receipt_dir / '2020-01-02.json').write_text(json.dumps({
        'dataset': 'TaiwanFuturesTick', 'data_id': '', 'partition': '2020-01-02',
        'status': 'complete', 'rows': 1, 'parquet_path': 'ticks.parquet',
        'parquet_size_bytes': payload.stat().st_size, 'sha256': '0' * 64,
    }))
    with sqlite3.connect(root / 'queue.sqlite3') as con:
        con.execute('CREATE TABLE tasks(dataset,data_id,partition,state,error_code)')
    summary, daily, unmatched = audit(official, sha256_file(official), root,
                                       start=date(2020, 1, 2), end=date(2020, 1, 2))
    assert daily[0]['status'] == 'receipt_or_payload_invalid'
    assert summary['official_contract_days_without_verified_provider_payload'] == 1
    assert unmatched == []
