from datetime import UTC, date, datetime, timedelta
from dataclasses import replace
import json
import hashlib
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import requests

from downloader import download_finmind_complement as worker
from downloader import download_finmind_sponsor as sponsor
from downloader import finmind_account as account
from downloader import finmind_supplemental as supplemental
from downloader.finmind_batching import coalesce_pending_tasks, split_batch_rows
from scripts.audit_finmind_query_ranges import registry
from downloader.finmind_scheduling import fixed_incremental_demand, _unspent_overseas_share
from stockagent.live.market_status import TwStockDayDecision


NOW = datetime(2026, 9, 30, 1, tzinfo=UTC)
DATASET = 'TaiwanStockConvertibleBondMonthlyAnalysis'


def test_market_history_one_task_without_current_bond_master_and_exact_old_evidence(tmp_path, monkeypatch):
    monkeypatch.setattr(supplemental, 'SOURCES', {DATASET: supplemental.SOURCES[DATASET]})
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [(DATASET, '13166', 'history', 'id_history', 2),
                                (DATASET, '17172', 'history', 'id_history', 2)])
        conn.execute("UPDATE tasks SET state='complete',rows=4,receipt_path='retained.json' WHERE data_id='13166'")
        before = conn.execute("SELECT * FROM tasks WHERE data_id='13166'").fetchone()
        names = [r[1] for r in conn.execute('PRAGMA table_info(tasks)')]
        for _ in range(2):
            supplemental.seed(conn, {}, NOW)
        assert conn.execute("SELECT data_id,state FROM tasks ORDER BY data_id").fetchall() == [
            ('', 'pending'), ('13166', 'deprecated_query_shape'), ('17172', 'deprecated_query_shape')]
        archived = conn.execute("SELECT prior_task_json FROM finmind_query_shape_migrations WHERE data_id='13166'").fetchone()[0]
        assert json.loads(archived) == dict(zip(names, before))
        assert conn.execute("SELECT count(*) FROM finmind_query_shape_migrations").fetchone()[0] == 2
        assert conn.execute("SELECT rows,receipt_path FROM tasks WHERE data_id='13166'").fetchone() == (4, 'retained.json')


def test_full_market_request_preserves_every_bond_and_observation_timestamp():
    endpoint, params, metadata = supplemental.request_contract(DATASET, '', 'history', NOW.date())
    assert endpoint.endswith('/data')
    assert params == {'dataset': DATASET, 'start_date': '2026-05-01', 'end_date': '2026-09-30'}
    assert metadata['query_shape'] == 'whole_market_inclusive_date_range'
    assert metadata['request_count'] == 1
    rows = [{'date': '2026-05-01', 'cb_id': '13166', 'change': -123},
            {'date': '2026-08-01', 'cb_id': 'NEW-ID', 'change': 0}]
    supplemental.validate_response(DATASET, '', 'history', NOW.date(), rows)
    assert rows[0]['change'] == -123
    assert registry()[DATASET]['query_shape'] == 'whole_market_date_range'
    with pytest.raises(ValueError, match='empty_identifier'):
        supplemental.request_contract(DATASET, '13166', 'history', NOW.date())
    for row in ({'date': '2026-08-01'}, {'date': '2026-04-01', 'cb_id': '13166'},
                {'date': '2026-10-01', 'cb_id': '13166'}):
        with pytest.raises(ValueError):
            supplemental.validate_response(DATASET, '', 'history', NOW.date(), [row])


def test_market_history_uses_next_documented_release_not_per_bond_renewals():
    task = worker.Task(DATASET, '', 'history', 'id_history', 0, 'complete')
    assert worker._next_refresh(task, NOW, empty=False) == '2026-09-30T10:00:00+00:00'
    saturday = datetime(2026, 10, 3, 10, tzinfo=UTC)
    assert worker._next_refresh(task, saturday, empty=False) == '2026-10-05T10:00:00+00:00'


def test_due_range_coalesces_both_directions_revisions_and_retries_without_gaps():
    seed = worker.Task('TaiwanBusinessIndicator', '', '2024-01-01', 'year', 1, 'complete')
    neighbors = [replace(seed, partition='2023-01-01', state='observed_empty'),
                 replace(seed, partition='2025-01-01', state='failed'),
                 replace(seed, partition='2026-01-01', state='pending')]
    batch = coalesce_pending_tasks(seed, neighbors, today=NOW.date(), include_due_refresh=True)
    assert [task.partition for task in batch.tasks] == [f'{y}-01-01' for y in range(2023, 2027)]
    assert batch.start_date == date(2023, 1, 1) and batch.end_date == NOW.date()
    assert sum(map(len, split_batch_rows(batch, [{'date': '2023-01-01'}, {'date': '2026-09-30'}]).values())) == 2
    bounded = coalesce_pending_tasks(seed, neighbors, today=NOW.date(), max_years=2, include_due_refresh=True)
    assert len(bounded.tasks) == 2
    oldest = replace(seed, partition='2023-01-01')
    upward = coalesce_pending_tasks(oldest, [seed, *neighbors], today=NOW.date(), include_due_refresh=True)
    assert len(upward.tasks) == 4


def test_sponsor_claims_only_due_revisions_and_preserves_not_due_hole(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        for year, state, due in [(2022, 'pending', None), (2023, 'complete', NOW + timedelta(days=1)),
                                 (2024, 'observed_empty', NOW), (2025, 'failed', NOW)]:
            conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,next_attempt_at_utc) "
                         "VALUES (?,'',?,'year',2,?,?)",
                         ('TaiwanBusinessIndicator', f'{year}-01-01', state, due.isoformat() if due else None))
        conn.commit()
        before = conn.execute("SELECT * FROM tasks WHERE partition='2023-01-01'").fetchone()
        task = sponsor._next(conn, NOW)
        batch = sponsor._claim_batch(conn, task, NOW)
        assert [t.partition for t in batch.tasks] == ['2024-01-01', '2025-01-01']
        assert conn.execute("SELECT * FROM tasks WHERE partition='2023-01-01'").fetchone() == before
        assert conn.execute("SELECT state FROM tasks WHERE partition='2022-01-01'").fetchone() == ('pending',)


class FakeSession:
    def __init__(self):
        self.calls = 0
        self.lock = threading.Lock()

    def get(self, _url, **kwargs):
        with self.lock:
            self.calls += 1
        time.sleep(0.02)
        class Response:
            def raise_for_status(self):
                pass
            def json(self):
                return {'status': 200, 'level_title': 'Sponsor', 'api_request_limit_hour': 6000,
                        'user_count': 123, 'email': 'never-persist@example.test'}
        return Response()


def test_account_sampling_is_single_flight_shared_and_does_not_forge_observation(tmp_path, monkeypatch):
    monkeypatch.setattr(account, '_CACHE', None)
    session = FakeSession()
    with ThreadPoolExecutor(max_workers=4) as pool:
        outputs = list(pool.map(lambda _: account.verified_account(session, 'test-secret', tmp_path), range(4)))
    assert session.calls == 1 and all(value == outputs[0] for value in outputs)
    monkeypatch.setattr(account, '_CACHE', None)  # another process has no in-memory cache
    assert account.verified_account(session, 'test-secret', tmp_path) == outputs[0]
    assert session.calls == 1
    for path in tmp_path.glob('*.json'):
        assert 'test-secret' not in path.read_text() and 'never-persist' not in path.read_text()
    assert 'credential_fingerprint' not in outputs[0]
    assert account.verified_account(session, 'rotated-secret', tmp_path)['tier'] == 'Sponsor'
    assert session.calls == 2  # token changes require fresh authority


@pytest.mark.parametrize('age', [61, -2])
def test_account_stale_or_future_sample_is_not_reused(tmp_path, monkeypatch, age):
    monkeypatch.setattr(account, '_CACHE', None)
    session = FakeSession()
    account.verified_account(session, 'test-secret', tmp_path)
    path = tmp_path / 'account_probe_cache.json'
    payload = json.loads(path.read_bytes())
    payload['account']['observed_at_utc'] = (datetime.now(UTC) - timedelta(seconds=age)).isoformat()
    path.write_text(json.dumps(payload))
    monkeypatch.setattr(account, '_CACHE', None)
    account.verified_account(session, 'test-secret', tmp_path)
    assert session.calls == 2


def test_account_expired_sample_is_not_returned_after_provider_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(account, '_CACHE', None)
    session = FakeSession()
    account.verified_account(session, 'test-secret', tmp_path)
    monkeypatch.setattr(account, '_CACHE', None)
    monkeypatch.setattr(account, '_cached_account', lambda *_: None)
    def unavailable(*_a, **_kw):
        raise requests.Timeout('unavailable')
    monkeypatch.setattr(session, 'get', unavailable)
    with pytest.raises(requests.Timeout):
        account.verified_account(session, 'test-secret', tmp_path)


def test_account_minute_bucket_does_not_delay_scheduled_minute_sample():
    stamp = datetime(2026, 9, 30, 0, 0, 50, tzinfo=UTC)
    assert account._fresh_observation(stamp, stamp + timedelta(seconds=5))
    assert not account._fresh_observation(stamp, stamp + timedelta(seconds=10))


def test_overseas_share_counts_only_its_recent_calls_and_protects_other_due_jobs(tmp_path):
    (tmp_path / 'account_status.json').write_text(json.dumps({'official_requests_per_hour': 40}))
    with sqlite3.connect(tmp_path / 'request_traffic.sqlite3') as conn:
        conn.execute('CREATE TABLE requests (started_at_utc TEXT, dataset TEXT)')
        conn.executemany('INSERT INTO requests VALUES (?,?)', [
            *((NOW.isoformat(), 'USStockPrice') for _ in range(7)),
            *((NOW.isoformat(), 'TaiwanStockNews') for _ in range(15)),
            ((NOW - timedelta(hours=1)).isoformat(), 'UKStockPrice'),
            ((NOW + timedelta(seconds=1)).isoformat(), 'JapanStockPrice'),
        ])
    assert _unspent_overseas_share(tmp_path, NOW) == 3
    for owner in ('sponsor', 'complement'):
        (tmp_path / owner).mkdir()
        with worker._db(tmp_path / owner / 'queue.sqlite3') as conn:
            worker._add_tasks(conn, [('USStockPrice', str(i), 'history', 'id_history', 0) for i in range(20)])
            worker._add_tasks(conn, [('TaiwanStockNews', '', '2026-09-30', 'day', 0)])
    closed = lambda *_a, **_kw: TwStockDayDecision('closed', 'fixture')
    # One calendar check + 20 due prices + one news job, aliases counted once.
    # Due-first scheduling supersedes the old permanently protected 25% share.
    assert fixed_incremental_demand(tmp_path, NOW, sources=(), day_decision=closed) == 22
    assert fixed_incremental_demand(tmp_path, NOW.astimezone(worker.TAIPEI),
                                    sources=(), day_decision=closed) == 22
    with sqlite3.connect(tmp_path / 'request_traffic.sqlite3') as conn:
        conn.executemany('INSERT INTO requests VALUES (?,?)', [(NOW.isoformat(), 'UKStockPrice')] * 5)
    assert fixed_incremental_demand(tmp_path, NOW, sources=(), day_decision=closed) == 22


@pytest.mark.parametrize('ledger_state', ['missing', 'broken_schema', 'unreadable'])
def test_overseas_share_never_credits_unknown_traffic(tmp_path, ledger_state):
    (tmp_path / 'account_status.json').write_text(json.dumps({'official_requests_per_hour': 6000}))
    ledger = tmp_path / 'request_traffic.sqlite3'
    if ledger_state == 'broken_schema':
        with sqlite3.connect(ledger) as conn:
            conn.execute('CREATE TABLE unrelated (n INTEGER)')
    elif ledger_state == 'unreadable':
        ledger.write_text('not a sqlite database')
    assert _unspent_overseas_share(tmp_path, NOW) == 1500


def test_overseas_share_falls_back_safely_when_quota_is_not_verified(tmp_path):
    assert _unspent_overseas_share(tmp_path, NOW) == 150
    for payload in ({'official_requests_per_hour': True}, {'official_requests_per_hour': -1}, [], 'bad'):
        (tmp_path / 'account_status.json').write_text(json.dumps(payload))
        assert _unspent_overseas_share(tmp_path, NOW) == 150


@pytest.mark.parametrize('boundary_kind', ['inclusive', 'ignored', 'exclusive'])
def test_market_history_boundary_acceptance_rejects_ignored_or_exclusive_end(tmp_path, boundary_kind):
    from scripts.audit_finmind_call_efficiency import verify_ranges
    root = tmp_path / 'private'
    report = tmp_path / 'report'
    root.mkdir()
    report.mkdir()
    rows = [{'cb_id': '13166', 'date': f'2026-{month:02}-01', 'change': -month} for month in range(5, 9)]
    rows.append({'cb_id': 'OTHER', 'date': '2026-06-01', 'change': 0})
    boundary = [r for r in rows if r['date'] <= '2026-07-01']
    if boundary_kind == 'ignored':
        boundary = rows
    elif boundary_kind == 'exclusive':
        boundary = [r for r in boundary if r['date'] < '2026-07-01']
    def probe(name, body):
        raw = json.dumps(body).encode()
        path = root / (name + '.json')
        path.write_bytes(raw)
        return {'name': name, 'payload_path': path.name, 'payload_sha256': hashlib.sha256(raw).hexdigest()}
    (report / 'contract_probes.json').write_text(json.dumps({'probes': [
        probe('bond_month_all', rows), probe('bond_month_one', [r for r in rows if r['cb_id'] == '13166'])]}))
    (report / 'range_boundary_probes.json').write_text(json.dumps({'probes': [probe('bond_month_boundary', boundary)]}))
    result = verify_ranges(root, report)
    assert result['accepted'] == (boundary_kind == 'inclusive')
