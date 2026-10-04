from datetime import UTC, date, datetime, timedelta
import json
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from downloader import download_finmind_complement as c
from scripts.probe_finmind_gold_range import compare_gold_rows


NOW = datetime(2026, 9, 27, 7, tzinfo=UTC)


def _years(conn, years, *, rows=0):
    c._add_tasks(conn, [("GoldPrice", "", str(year), "year", 0 if year == NOW.year else 1)
                        for year in years])
    conn.execute("UPDATE tasks SET rows=? WHERE dataset='GoldPrice'", (rows,))
    conn.commit()


def _task(conn, year):
    return c.Task(*conn.execute(
        "SELECT dataset,data_id,partition,kind,priority,state FROM tasks WHERE dataset='GoldPrice' AND partition=?",
        (str(year),),
    ).fetchone())


def test_gold_retains_last_day_intraday_and_discards_only_next_midnight(tmp_path):
    with c._db(tmp_path / 'queue.sqlite3') as conn:
        _years(conn, [2016, 2017])
        batch = c._claim_bulk_years(conn, _task(conn, 2017), NOW, allow_history=True)
        assert batch.params() == {'dataset': 'GoldPrice', 'start_date': '2016-01-01', 'end_date': '2018-01-01'}
        rows = [
            {'date': '2016-12-31 23:59:59.999999', 'Price': 1200.123456},
            {'date': '2017-01-01', 'Price': 1201.123456},
            {'date': '2017-12-31 23:59:59.999999', 'Price': 1303.123456},
            {'date': '2018-01-01 00:00:00', 'Price': 1304.123456},
        ]
        assert c._store_bulk_years(conn, tmp_path, batch.tasks[-1], rows, NOW, NOW.date(), batch=batch) == 3
        c._release_bulk_years(conn, batch)
        stored = []
        for year in [2016, 2017]:
            receipt = json.loads((tmp_path / f'receipts/GoldPrice/all/{year}.json').read_text())
            stored.extend(pq.read_table(tmp_path / receipt['parquet_path']).to_pylist())
            meta = receipt['request']
            assert meta['query_shape'] == 'whole_market_naive_timestamp_range_client_half_open'
            assert meta['request_end_date'] == '2018-01-01'
            assert meta['covered_end_date'] == '2017-12-31'
            assert meta['covered_end_timestamp_exclusive'] == '2018-01-01 00:00:00'
            assert meta['response_rows'] == 4 and meta['stored_rows'] == 3
            assert meta['excluded_next_midnight_rows'] == 1
            assert meta['response_partition_rows'] == {'2016': 1, '2017': 2}
        assert stored == rows[:-1]


def test_single_year_request_uses_same_clock_and_body_limit(tmp_path, monkeypatch):
    calls = []
    expected = [{'date': '2017-12-31 23:57:00', 'Price': 1303.0}]
    monkeypatch.setattr(c, '_fetch_rows', lambda *_a, **_kw: calls.append((_a[-1], _kw)) or
                        [*expected, {'date': '2018-01-01 00:00:00', 'Price': 1304.0}])
    task = c.Task('GoldPrice', '', '2017', 'year', 1, 'pending')
    assert c._request(object(), object(), tmp_path, task, 'placeholder', today=NOW.date()) == expected
    assert calls == [({'dataset': 'GoldPrice', 'start_date': '2017-01-01', 'end_date': '2018-01-01'},
                      {'max_response_bytes': c.BULK_MAX_RESPONSE_BYTES})]


@pytest.mark.parametrize('stamp,code', [
    ('2018-01-01 00:00:00.000001', 'invalid_bulk_range'),
    ('2015-12-31 23:59:59', 'invalid_bulk_range'),
    ('2017-12-31 23:59:59+00:00', 'invalid_bulk_date'),
    ('2017-02-30 00:00:00', 'invalid_bulk_date'),
])
def test_gold_rejects_other_outside_rows_and_unproven_clocks_before_any_store(tmp_path, stamp, code):
    with c._db(tmp_path / 'queue.sqlite3') as conn:
        _years(conn, [2016, 2017])
        batch = c._claim_bulk_years(conn, _task(conn, 2017), NOW, allow_history=True)
        with pytest.raises(c.SourceError, match=code):
            c._store_bulk_years(conn, tmp_path, batch.tasks[-1],
                               [{'date': '2016-01-01', 'Price': 1.0}, {'date': stamp, 'Price': 2.0}],
                               NOW, NOW.date(), batch=batch)
        assert not (tmp_path / 'receipts').exists()


def test_current_gold_under_reserve_claims_only_one_year_and_uses_next_midnight(tmp_path):
    with c._db(tmp_path / 'queue.sqlite3') as conn:
        _years(conn, [2024, 2025, 2026])
        batch = c._claim_bulk_years(conn, _task(conn, 2026), NOW, allow_history=False)
        assert [task.partition for task in batch.tasks] == ['2026']
        assert batch.params()['end_date'] == '2026-09-28'
        assert batch.metadata()['covered_end_date'] == '2026-09-27'
        assert batch.metadata()['current_day_is_partial'] is True
        assert conn.execute("SELECT partition FROM tasks WHERE state='pending' ORDER BY partition").fetchall() == [
            ('2024',), ('2025',),
        ]


def test_empty_after_discarding_boundary_cannot_overwrite_existing_nonempty_receipt(tmp_path):
    with c._db(tmp_path / 'queue.sqlite3') as conn:
        _years(conn, [2017])
        task = _task(conn, 2017)
        receipt = c._store(tmp_path, task, [{'date': '2017-12-31 00:00:00', 'Price': 1303.0}], NOW)
        c._save_result(conn, task, receipt, NOW)
        path = tmp_path / receipt['receipt_path']
        old = path.read_bytes()
        conn.execute("UPDATE tasks SET next_attempt_at_utc=?", (NOW.isoformat(),))
        conn.commit()
        batch = c._claim_bulk_years(conn, _task(conn, 2017), NOW, allow_history=True)
        with pytest.raises(c.SourceError, match='incomplete_bulk_response'):
            c._store_bulk_years(conn, tmp_path, task, [{'date': '2018-01-01 00:00:00', 'Price': 1304.0}],
                               NOW, NOW.date(), batch=batch)
        assert path.read_bytes() == old
        assert conn.execute("SELECT rows FROM tasks").fetchone() == (1,)


def test_legacy_migration_reopens_all_127_years_once_without_touching_old_receipts(tmp_path):
    with c._db(tmp_path / 'queue.sqlite3') as conn:
        _years(conn, range(1900, 2027))
        originals = {}
        for year in range(1900, 2027):
            task = _task(conn, year)
            rows = [{'date': f'{year}-01-01', 'Price': 1000.0}] if year >= 1977 else []
            receipt = c._store(tmp_path, task, rows, NOW)
            c._save_result(conn, task, receipt, NOW)
            originals[receipt['receipt_path']] = (tmp_path / receipt['receipt_path']).read_bytes()
        assert c._migrate_gold_timestamp_boundary(conn, tmp_path, NOW) == 127
        conn.commit()
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone() == (127,)
        assert conn.execute("SELECT COUNT(*) FROM gold_timestamp_boundary_migration").fetchone() == (127,)
        audit = conn.execute("SELECT prior_task_json,prior_receipt_json FROM gold_timestamp_boundary_migration "
                             "WHERE partition='2017'").fetchone()
        assert json.loads(audit[0])['state'] == 'complete'
        assert audit[1].encode() == originals['receipts/GoldPrice/all/2017.json']
        assert c._migrate_gold_timestamp_boundary(conn, tmp_path, NOW + timedelta(minutes=1)) == 0
        assert all((tmp_path / path).read_bytes() == original for path, original in originals.items())
        batch = c._claim_bulk_years(conn, _task(conn, 2026), NOW, allow_history=True)
        assert len(batch.tasks) == 127
        assert batch.params() == {'dataset': 'GoldPrice', 'start_date': '1900-01-01', 'end_date': '2026-09-28'}


def test_verified_clock_is_not_migrated_and_not_due_year_breaks_batch(tmp_path):
    with c._db(tmp_path / 'queue.sqlite3') as conn:
        _years(conn, [2014, 2015, 2016, 2017])
        task = _task(conn, 2015)
        single = c._GlobalYearBatch('GoldPrice', 'year', (task,), date(2015, 1, 1), date(2015, 12, 31), NOW.date())
        receipt = c._store(tmp_path, task, [{'date': '2015-12-31 23:59:59', 'Price': 1100.0}], NOW,
                           request_metadata=single.metadata())
        c._save_result(conn, task, receipt, NOW)
        before = conn.execute("SELECT * FROM tasks WHERE partition='2015'").fetchone()
        assert c._migrate_gold_timestamp_boundary(conn, tmp_path, NOW) == 0
        batch = c._claim_bulk_years(conn, _task(conn, 2017), NOW, allow_history=True)
        assert [part.partition for part in batch.tasks] == ['2016', '2017']
        assert conn.execute("SELECT * FROM tasks WHERE partition='2015'").fetchone() == before
        assert conn.execute("SELECT state FROM tasks WHERE partition='2014'").fetchone() == ('pending',)


def test_gold_learned_single_year_limit_keeps_timestamp_contract_and_restart_recovery(tmp_path):
    with c._db(tmp_path / 'queue.sqlite3') as conn:
        _years(conn, [2016, 2017])
        batch = c._claim_bulk_years(conn, _task(conn, 2017), NOW, allow_history=True)
        c._fail_bulk_years(conn, batch, c.SourceError('response_size_limit', retry_after=0), NOW)
        c._release_bulk_years(conn, batch)
        retry = c._claim_bulk_years(conn, _task(conn, 2017), NOW + timedelta(seconds=61), allow_history=True)
        assert len(retry.tasks) == 1 and retry.params()['end_date'] == '2018-01-01'
        c._recover_bulk_year_claims(conn)
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone() == (2,)
        assert c._migrate_gold_timestamp_boundary(conn, tmp_path, NOW) == 0


@pytest.mark.parametrize('history_allowed', [True, False])
def test_filtered_worker_one_http_preserves_end_day_and_quota_reservation(tmp_path, monkeypatch, history_allowed):
    monkeypatch.setenv('FINMIND_TOKEN', 'test-placeholder')
    monkeypatch.setattr(c, '_now', lambda: NOW)
    monkeypatch.setattr(c, 'load_env_file', lambda *_a, **_kw: None)
    monkeypatch.setattr(c, 'verified_account', lambda *_a: {'tier': 'Free', 'official_requests_per_hour': 600})
    monkeypatch.setattr(c, 'rate_limiter', lambda *_a: object())
    monkeypatch.setattr(c, '_populate', lambda *_a, **_kw: None)
    monkeypatch.setattr(c, 'fixed_incremental_demand', lambda *_a: 0)
    monkeypatch.setattr(c, 'backfill_budget', lambda *_a, **_kw: {'allowed': history_allowed})
    monkeypatch.setattr(c.shutil, 'disk_usage', lambda *_a: SimpleNamespace(free=100 * 1024**3))
    monkeypatch.setattr(c, '_status', lambda conn, root, **kw: kw)
    with c._db(tmp_path / 'queue.sqlite3') as conn:
        _years(conn, [2025, 2026])
        c._add_tasks(conn, [('TaiwanStockInfo', '', 'latest', 'snapshot', 0)])
        before = conn.execute("SELECT * FROM tasks WHERE dataset='TaiwanStockInfo'").fetchone()
    calls = []
    rows = ([{'date': '2025-12-31 23:59:59', 'Price': 2000.1}] if history_allowed else []) + [
        {'date': '2026-09-27 07:00:00', 'Price': 2100.123456},
        {'date': '2026-09-28 00:00:00', 'Price': 2101.234567},
    ]
    monkeypatch.setattr(c, '_fetch_rows', lambda *_a, **_kw: calls.append((_a[-1], _kw)) or rows)
    result = c.run_once(tmp_path, max_requests=1, datasets=('GoldPrice',))
    assert calls == [({'dataset': 'GoldPrice', 'start_date': '2025-01-01' if history_allowed else '2026-01-01',
                      'end_date': '2026-09-28'}, {'max_response_bytes': c.BULK_MAX_RESPONSE_BYTES})]
    assert result['last']['rows'] == (2 if history_allowed else 1)
    assert result['last']['request_batch']['response_rows'] == len(rows)
    with c._db(tmp_path / 'queue.sqlite3') as conn:
        assert conn.execute("SELECT * FROM tasks WHERE dataset='TaiwanStockInfo'").fetchone() == before
        assert conn.execute("SELECT state FROM tasks WHERE dataset='GoldPrice' AND partition='2025'").fetchone() == (
            'complete' if history_allowed else 'pending',)
        assert conn.execute("SELECT COUNT(*) FROM complement_year_batch_claims").fetchone() == (0,)
    receipt = json.loads((tmp_path / 'receipts/GoldPrice/all/2026.json').read_text())
    assert pq.read_table(tmp_path / receipt['parquet_path']).to_pylist() == [rows[-2]]


def test_probe_normalizes_only_timestamp_keys_and_reports_raw_rendering_difference():
    local = [{'date': '2016-01-01', 'Price': 1060.0},
             {'date': '2017-12-31 00:00:00', 'Price': 1303.0},
             {'date': '2018-01-02 00:00:00', 'Price': 1306.8},
             {'date': '2018-01-02 00:02:00', 'Price': 1306.7}]
    actual = [{'date': '2016-01-01 00:00:00', 'Price': 1060.0},
              {'date': '2017-12-31 00:00:00', 'Price': 1303.0},
              {'date': '2017-12-31 23:57:00', 'Price': 1303.0},
              {'date': '2018-01-02 00:00:00', 'Price': 1306.8}]
    result = compare_gold_rows(local, actual)
    assert result['contract_verified'] is True
    assert result['raw_old_rows_preserved'] is False
    assert result['raw_date_rendering_mismatch_rows'] == 1
    assert result['normalized_old_rows_preserved'] is True
    assert result['annual_final_day_repair_rows'] == 1
    assert local[0]['date'] == '2016-01-01'
