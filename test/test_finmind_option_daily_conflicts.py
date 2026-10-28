from datetime import UTC, date, datetime, timedelta
import csv
import hashlib
import io
import json
import sqlite3
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader import finmind_corrections as corrections
from downloader.finmind_option_daily_conflicts import load_daily_option_conflicts, validate_priority_keys
from downloader import download_finmind_sponsor as sponsor
from downloader import finmind_option_daily_conflicts as daily_conflicts
from scripts.request_finmind_option_daily_conflicts import register


NOW = datetime(2026, 10, 28, 15, tzinfo=UTC)
DAY = '2026-10-02'


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj), encoding='utf-8')


def _table(path, *, volume=0, keep_old=True):
    path.parent.mkdir(parents=True, exist_ok=True)
    products = ['TXO', 'TGO'] if keep_old else ['TGO']
    pq.write_table(pa.table({'option_id': products, 'contract_date': ['202610'] * len(products),
                             'strike_price': [100.] * len(products), 'call_put': ['call'] * len(products),
                             'trading_session': ['position'] * len(products),
                             'volume': ([2, volume] if keep_old else [volume])}), path)
    return len(products)


def _setup(tmp_path):
    root = tmp_path / 'data_finmind/sponsor'
    root.mkdir(parents=True)
    relative = 'parquet/TaiwanOptionDaily/all/2026-10-02/old.parquet'
    original = root / relative
    _table(original)
    receipt_path = root / 'receipts/TaiwanOptionDaily/all/2026-10-02.json'
    receipt = {'dataset': 'TaiwanOptionDaily', 'data_id': '', 'partition': DAY, 'kind': 'day',
               'status': 'complete', 'rows': 2, 'fetched_at_utc': (NOW - timedelta(days=1)).isoformat(),
               'parquet_path': relative, 'parquet_size_bytes': original.stat().st_size, 'sha256': _sha(original)}
    _write_json(receipt_path, receipt)
    with sqlite3.connect(root / 'queue.sqlite3') as con:
        con.execute('''CREATE TABLE tasks(dataset TEXT,data_id TEXT,partition TEXT,kind TEXT,priority INTEGER,
            state TEXT,next_attempt_at_utc TEXT,last_attempt_at_utc TEXT,rows INTEGER,bytes INTEGER,
            first_data_date TEXT,last_data_date TEXT,receipt_path TEXT,error_code TEXT,
            PRIMARY KEY(dataset,data_id,partition))''')
        con.execute('INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    ('TaiwanOptionDaily', '', DAY, 'day', 2, 'complete', None, receipt['fetched_at_utc'],
                     2, original.stat().st_size, DAY, DAY, str(receipt_path.relative_to(root)), None))
    source = tmp_path / 'data_tw_index_options_daily/oct.csv'
    source.parent.mkdir(parents=True)
    source.write_bytes(b'official source')
    source_receipt = source.with_suffix('.receipt.json')
    _write_json(source_receipt, {'contract': 'taifex_option_all_products_daily_raw_v1',
                'range_start': '2026-10-01', 'range_end': '2026-10-31', 'point_in_time_snapshot': False,
                'path': str(source), 'sha256': _sha(source), 'bytes': source.stat().st_size})
    coverage = tmp_path / 'artifacts/markets/coverage.json'
    _write_json(coverage, {'contract': 'tw_option_all_product_monthly_range_readback_v1',
                'sources': [{'start': '2026-10-01', 'end': '2026-10-31', 'path': str(source),
                             'receipt': str(source_receipt), 'sha256': _sha(source), 'bytes': source.stat().st_size}],
                'daily_positive_products': [{'date': DAY, 'day_positive_products': 'TGO|TXO'}]})
    inventory = tmp_path / 'artifacts/markets/products.csv'
    row = {'date': DAY, 'source_status': 'daily_readback_verified', 'source_kind': 'day',
           'receipt_sha256': _sha(receipt_path), 'parquet_sha256': _sha(original),
           'official_day_positive_products': 'TGO|TXO', 'vendor_products': 'TGO|TXO',
           'vendor_positive_volume_products': 'TXO', 'missing_day_positive_volume_products': 'TGO'}
    buf = io.StringIO(newline='')
    writer = csv.DictWriter(buf, fieldnames=list(row)); writer.writeheader(); writer.writerow(row)
    inventory.write_text(buf.getvalue())
    summary = tmp_path / 'artifacts/markets/product_summary.json'
    _write_json(summary, {'contract': 'taifex_option_vs_finmind_all_market_daily_presence_v2',
                'finmind_daily_root': str(root), 'day_results': str(inventory), 'day_results_sha256': _sha(inventory),
                'official_coverage_summary': str(coverage), 'official_coverage_summary_sha256': _sha(coverage)})
    return root, summary, source


def test_exact_conflict_is_registered_without_provider_call_and_requires_repair(tmp_path):
    root, summary, _ = _setup(tmp_path)
    result = register(root, summary, NOW)
    assert result['provider_calls'] == 0
    assert result['correction_adoption']['applied'] == result['date_count'] == 1
    assert result['priority']['items'][0]['state'] == 'pending'
    with pytest.raises(FileExistsError):
        register(root, summary, NOW)
    plan, _ = load_daily_option_conflicts(root, NOW)
    request = plan['requests'][0]
    assert request['allow_empty'] is False
    assert request['expected_source_positive_volume_products'] == ['TGO', 'TXO']
    with sqlite3.connect(root / 'queue.sqlite3') as con:
        task = {'dataset': 'TaiwanOptionDaily', 'data_id': '', 'partition': DAY, 'kind': 'day', 'rows': 2}
        context = corrections.correction_context(con, task)
    refreshed = root / 'parquet/TaiwanOptionDaily/all/2026-10-02/new.parquet'
    _table(refreshed, volume=0)
    receipt = {**task, 'status': 'complete', 'fetched_at_utc': (NOW + timedelta(seconds=1)).isoformat(),
               'parquet_path': str(refreshed.relative_to(root)), 'parquet_size_bytes': refreshed.stat().st_size,
               'sha256': _sha(refreshed), **corrections.correction_receipt_metadata(context)}
    assert corrections._verified_receipt(root, task, request, 'sponsor', receipt) is None
    _table(refreshed, volume=3)
    receipt.update(parquet_size_bytes=refreshed.stat().st_size, sha256=_sha(refreshed))
    assert corrections._verified_receipt(root, task, request, 'sponsor', receipt)['verified_positive_volume_products'] == 2
    task['rows'] = _table(refreshed, volume=3, keep_old=False)
    receipt.update(rows=1, parquet_size_bytes=refreshed.stat().st_size, sha256=_sha(refreshed))
    assert corrections._verified_receipt(root, task, request, 'sponsor', receipt) is None


def test_changed_official_source_is_not_admitted(tmp_path):
    root, summary, source = _setup(tmp_path)
    register(root, summary, NOW)
    source.write_bytes(b'changed source')
    with pytest.raises(ValueError, match='official source evidence changed'):
        load_daily_option_conflicts(root, NOW)


def test_exact_owner_selection_cannot_escape_registered_dates_or_expiry(tmp_path):
    root, summary, _ = _setup(tmp_path)
    result = register(root, summary, NOW)
    request_id = result['priority']['request_id']
    keys = (('TaiwanOptionDaily', '', DAY),)
    validate_priority_keys(root, request_id, keys, NOW)
    with pytest.raises(ValueError, match='active exact priority'):
        validate_priority_keys(root, request_id, (('TaiwanOptionDaily', '', '2026-10-03'),), NOW)
    with pytest.raises(ValueError, match='inactive'):
        validate_priority_keys(root, request_id, keys, NOW + timedelta(days=7))
    with sqlite3.connect(root / 'queue.sqlite3') as con:
        con.execute('INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    ('TaiwanOptionDaily', '', '2026-10-03', 'day', 0, 'pending', None, None,
                     0, 0, None, None, None, None))
        selected = sponsor._next(con, NOW, datasets=('TaiwanOptionDaily',), task_keys=keys)
        assert (selected.dataset, selected.data_id, selected.partition) == keys[0]
        assert con.execute("SELECT state FROM tasks WHERE partition='2026-10-03'").fetchone() == ('pending',)


def test_finite_capacity_requires_real_physical_measurement(tmp_path, monkeypatch):
    from scripts import check_acquisition_capacity
    monkeypatch.setattr(daily_conflicts, 'validate_priority_keys', lambda *args: None)
    observed = {}
    def probe(root, *, additional_headroom_bytes):
        observed['headroom'] = additional_headroom_bytes
        return {'state': 'not_enrolled'}
    monkeypatch.setattr(check_acquisition_capacity, 'priority_dispatch_status', probe)
    keys = (('TaiwanOptionDaily', '', DAY),)
    assert daily_conflicts.capacity_probe(tmp_path, 'x', keys)['state'] == 'deferred'
    assert observed['headroom'] == 24 * 1024**3 + 64 * 1024**2
    monkeypatch.setattr(check_acquisition_capacity, 'priority_dispatch_status',
                        lambda *args, **kw: {'state': 'admitted', 'backing_volume': {
                            'observation_basis': 'same_dispatch_physical_probe'}})
    assert daily_conflicts.capacity_probe(tmp_path, 'x', keys)['guard_kind'] == 'finite_option_ordinary_physical_capacity_deferred'


def test_finite_owner_is_one_request_one_worker_and_fetch_is_bounded(tmp_path, monkeypatch):
    keys = (('TaiwanOptionDaily', '', DAY),)
    with pytest.raises(ValueError, match='invalid keys'):
        sponsor.run_once(tmp_path, workers=2, max_requests=1, task_keys=keys, task_keys_validate=lambda: None)
    with pytest.raises(ValueError, match='requires its source-bound'):
        sponsor.run_once(tmp_path, workers=1, max_requests=1, task_keys=keys)
    monkeypatch.setattr(sponsor._THREAD, 'session', object(), raising=False)
    kwargs = {}
    def fetch(*args, **options):
        kwargs.update(options)
        return [{'date': DAY}]
    monkeypatch.setattr(sponsor, '_fetch_rows', fetch)
    task = sponsor.Task('TaiwanOptionDaily', '', DAY, 'day', 1, 'pending')
    assert sponsor._fetch(task, 'token', None, tmp_path, date(2026, 10, 28), max_response_bytes=64 * 1024**2)
    assert kwargs == {'max_response_bytes': 64 * 1024**2}


def test_daily_priority_reservation_yields_only_due_foreign_history(tmp_path):
    from downloader import download_finmind_complement as complement

    root = tmp_path / 'data_finmind/sponsor'
    root.mkdir(parents=True)
    queue = root.parent / 'complement/queue.sqlite3'
    queue.parent.mkdir()
    due = (NOW - timedelta(hours=1)).isoformat()
    with complement._db(queue) as con:
        complement._add_tasks(con, [('USStockPrice', 'US1', 'history', 'id_history', 0),
                                    ('TaiwanStockInfo', '', 'latest', 'snapshot', 0)])
        con.execute('UPDATE tasks SET next_attempt_at_utc=?', (due,))
        before = con.execute('SELECT * FROM tasks ORDER BY dataset,data_id').fetchall()
    reservation = {'observed_at_utc': NOW.isoformat(), 'ready_requests': 2, 'reserve_requests': 5,
                   'upcoming_requests': 3, 'queue_errors': [],
                   'events': [{'owner': 'complement', 'dataset': 'USStockPrice', 'due_at_utc': due, 'requests': 1},
                              {'owner': 'complement', 'dataset': 'TaiwanStockInfo', 'due_at_utc': due, 'requests': 1}]}
    selected = daily_conflicts.priority_reservation(root, reservation, NOW)
    assert selected['preempted_foreign_history_requests'] == 1
    assert (selected['ready_requests'], selected['reserve_requests'], selected['upcoming_requests']) == (1, 4, 3)
    assert reservation['events'][0]['requests'] == 1
    with sqlite3.connect(queue) as con:
        assert con.execute('SELECT * FROM tasks ORDER BY dataset,data_id').fetchall() == before
    stale = dict(reservation, observed_at_utc=(NOW - timedelta(seconds=1)).isoformat())
    assert daily_conflicts.priority_reservation(root, stale, NOW) is stale
    assert daily_conflicts.priority_reservation(tmp_path / 'missing/sponsor', reservation, NOW) is reservation


@pytest.mark.parametrize('mode', ['ordinary_budget', 'capacity_deferred', 'missing_capacity_proof'])
def test_finite_owner_dispatches_only_selected_date_with_normal_receipts(tmp_path, monkeypatch, mode):
    root, summary, _ = _setup(tmp_path)
    register(root, summary, NOW)
    monkeypatch.setattr(sponsor, 'load_env_file', lambda *_a, **_kw: None)
    monkeypatch.setattr(sponsor, 'verified_account', lambda *_a: {'tier': 'Sponsor', 'official_requests_per_hour': 6000})
    monkeypatch.setattr(sponsor, 'refresh_dispatch_account', lambda account, *_a: account)
    monkeypatch.setattr(sponsor, 'rate_limiter', lambda *_a: object())
    monkeypatch.setattr(sponsor, '_seed', lambda *_a, **_kw: {'state': 'fixture'})
    monkeypatch.setattr(sponsor, '_official_price_coverage', lambda: None)
    monkeypatch.setattr(sponsor, '_official_session_calendar', lambda: None)
    budget = ({'allowed': True} if mode == 'ordinary_budget' else
              {'allowed': False, 'priority_wait': True, 'schedule_verified': True,
               'basis': 'provider_observation_plus_local_starts', 'remaining': 1000, 'reserve': 10})
    monkeypatch.setattr(sponsor, 'backfill_budget', lambda *_a, **_kw: budget)
    reservation = {'reserve_requests': 8, 'ready_requests': 8, 'queue_errors': []}
    monkeypatch.setattr(sponsor, 'incremental_reservation', lambda *_a: reservation)
    filtered = []
    def filter_reservation(actual_root, actual_reservation, _now):
        filtered.append((actual_root, actual_reservation))
        return actual_reservation
    monkeypatch.setattr(daily_conflicts, 'priority_reservation', filter_reservation)
    monkeypatch.setattr(sponsor, 'protected_stock_opening', lambda *_a: False)
    monkeypatch.setattr(sponsor.shutil, 'disk_usage', lambda *_a: SimpleNamespace(free=100 * 1024**3))
    admission = {'state': 'admitted', 'sample_age_seconds': 0}
    if mode == 'capacity_deferred':
        admission['guard_kind'] = 'finite_option_ordinary_physical_capacity_deferred'
    capacity = SimpleNamespace(root=root.absolute(), initial_sample=lambda **_kw: admission,
                               sample=lambda: admission)
    calls = []
    def fetch(*args, **kwargs):
        calls.append((args[-1], kwargs))
        return [{'date': DAY, 'option_id': symbol, 'contract_date': '202610', 'strike_price': 100.,
                 'call_put': 'call', 'trading_session': 'position', 'volume': 2} for symbol in ('TXO', 'TGO')]
    monkeypatch.setattr(sponsor, '_fetch_rows', fetch)
    with sqlite3.connect(root / 'queue.sqlite3') as con:
        con.execute('INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    ('TaiwanOptionDaily', '', '2026-10-03', 'day', 0, 'pending', None, None,
                     0, 0, None, None, None, None))
    result = sponsor.run_once(root, max_requests=1, workers=1, capacity_sampler=capacity,
                              task_keys=(('TaiwanOptionDaily', '', DAY),), task_keys_validate=lambda: None)
    assert filtered and filtered[0] == (root, reservation)
    if mode == 'missing_capacity_proof':
        assert not calls
    else:
        assert result['last_task']['partition'] == DAY
        assert len(calls) == 1 and calls[0][1]['max_response_bytes'] == 64 * 1024**2
    with sqlite3.connect(root / 'queue.sqlite3') as con:
        assert con.execute("SELECT state FROM tasks WHERE partition='2026-10-03'").fetchone() == ('pending',)
        expected = 'queued' if mode == 'missing_capacity_proof' else 'repaired'
        assert con.execute("SELECT state FROM finmind_correction_tasks WHERE partition=?", (DAY,)).fetchone() == (expected,)
