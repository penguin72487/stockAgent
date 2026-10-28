from datetime import UTC, datetime, timedelta
import hashlib
import json

import pytest

from downloader import download_finmind_complement as worker
from downloader import finmind_product_query_verification as verification

NOW = datetime(2026, 10, 26, 23, tzinfo=UTC)
KEY = ('TaiwanFuturesDaily', 'FC1', 'history')


def plan(tmp_path, **changes):
    source = tmp_path / 'source-readback.json'
    source.write_text('{"no_verified_positive_trade":true}')
    value = dict(contract=verification.CONTRACT, query_shape='per_id_full_history_verification',
        authorization='User explicitly requests exact missing-source checks through the priority downloader',
        reason='Compare explicit product response with retained whole-market observations',
        source_only_no_training_injection=True,
        source_evidence=[dict(path=str(source), sha256=hashlib.sha256(source.read_bytes()).hexdigest())],
        keys=[dict(zip(('dataset','data_id','partition'), KEY))])
    value.update(changes)
    path = tmp_path / 'verification.json'
    path.write_text(json.dumps(value))
    return path


def seed(conn):
    worker._add_tasks(conn, [(*KEY, 'id_history', 3),
        ('TaiwanFuturesDaily', 'TX', 'history', 'id_history', 3),
        ('GoldPrice', '', '2000', 'year', 3)])


def test_exact_verification_shape_is_not_hidden_by_dataset_delegation(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        seed(conn)
        before = conn.execute('SELECT * FROM tasks ORDER BY dataset,data_id').fetchall()
        path = plan(tmp_path)
        assert verification.register(conn, path, NOW)['registered_keys'] == 1
        assert verification.register(conn, path, NOW)['task_states_changed'] is False
        assert conn.execute('SELECT * FROM tasks ORDER BY dataset,data_id').fetchall() == before
        delegated = frozenset({KEY[0]})
        selected = worker._next_task(conn, NOW, delegated=delegated)
        assert (selected.dataset, selected.data_id, selected.partition) == KEY
        assert worker._next_task(conn, NOW, delegated=delegated, required_keys=(KEY,)).data_id == 'FC1'
        assert worker._next_task(conn, NOW, delegated=delegated,
            required_keys=((KEY[0], 'TX', 'history'),)) is None
        assert worker._next_task(conn, NOW, delegated=delegated, incremental_only=True) is None


def test_product_verification_accepts_more_than_64_exact_keys_and_coalesces_duplicates(tmp_path):
    keys = [dict(dataset='TaiwanFuturesDaily', data_id=f'P{i:04d}', partition='history')
            for i in range(100)]
    path = plan(tmp_path, keys=keys + keys[:10])
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [(item['dataset'], item['data_id'], item['partition'], 'id_history', 3)
                                for item in keys])
        result = verification.register(conn, path, NOW)
        assert result['registered_keys'] == len(keys) and result['provider_calls'] == 0
        assert conn.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone()[0] == len(keys)
        assert verification.permits(conn, ('TaiwanFuturesDaily', 'P0099', 'history'))


def test_due_refresh_precedes_the_source_check_and_scope_is_exact(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        seed(conn); verification.register(conn, plan(tmp_path), NOW)
        worker._add_tasks(conn, [('TaiwanStockInfo', '', 'latest', 'snapshot', 0)])
        assert worker._next_task(conn, NOW, delegated=frozenset({KEY[0]})).dataset == 'TaiwanStockInfo'
        assert worker._next_task(conn, NOW, delegated=frozenset({KEY[0]}), datasets=('GoldPrice',)).dataset == 'GoldPrice'


def reservation(events, *, upcoming=0):
    ready = sum(row['requests'] for row in events if row['due_at_utc'] <= NOW.isoformat())
    return dict(observed_at_utc=NOW.isoformat(), ready_requests=ready,
                upcoming_requests=upcoming, reserve_requests=ready + upcoming,
                queue_errors=[], events=events)


def accepted_refresh(conn, dataset='USStockPrice', identifier='OLD', *, state='complete', due=None):
    worker._add_tasks(conn, [(dataset, identifier, 'history', 'id_history', 0)])
    conn.execute('UPDATE tasks SET state=?,next_attempt_at_utc=? WHERE dataset=? AND data_id=?',
                 (state, due or (NOW-timedelta(hours=1)).isoformat(), dataset, identifier))


def test_finite_source_priority_preempts_due_foreign_history_without_changing_tasks(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        seed(conn); verification.register(conn, plan(tmp_path), NOW)
        for identifier, state in [('DONE', 'complete'), ('EMPTY', 'observed_empty'),
                                  ('MISSING', 'pending'), ('RETRY', 'failed')]:
            accepted_refresh(conn, identifier=identifier, state=state)
        accepted_refresh(conn, 'EuropeStockPrice', 'EUROPE')
        accepted_refresh(conn, 'JapanStockPrice', 'DELEGATED')
        due = (NOW-timedelta(hours=1)).isoformat()
        original = reservation([
            dict(owner='complement', dataset='USStockPrice', due_at_utc=due, requests=4),
            dict(owner='complement', dataset='EuropeStockPrice', due_at_utc=due, requests=1),
            dict(owner='sponsor', dataset='USStockPrice', due_at_utc=due, requests=5),
            dict(owner='complement', dataset='JapanStockPrice', due_at_utc=due, requests=1),
        ], upcoming=7)
        before = conn.execute('SELECT * FROM tasks ORDER BY dataset,data_id').fetchall()
        selected = verification.prioritize_source_checks(conn, original, NOW, frozenset({'JapanStockPrice'}))
        assert selected['ready_requests'] == original['ready_requests'] - 5
        assert selected['reserve_requests'] == original['reserve_requests'] - 5
        assert selected['upcoming_requests'] == 7
        assert selected['preempted_foreign_history_requests'] == 5
        assert [event['requests'] for event in selected['events']] == [0, 0, 5, 1]
        assert original['events'][0]['requests'] == 4
        assert conn.execute('SELECT * FROM tasks ORDER BY dataset,data_id').fetchall() == before


def test_finite_priority_obeys_real_release_quota_and_future_reservations(tmp_path):
    from downloader.finmind_account import backfill_budget

    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        seed(conn); verification.register(conn, plan(tmp_path), NOW)
        accepted_refresh(conn)
        original = reservation([dict(owner='complement', dataset='USStockPrice', requests=1,
                                     due_at_utc=(NOW-timedelta(hours=1)).isoformat())], upcoming=10)
        selected = verification.prioritize_source_checks(conn, original, NOW, frozenset())
        account = dict(official_requests_per_hour=600, provider_used_in_hour=0,
                       observed_at_utc=NOW.isoformat())

        def budget(value, current_account=account):
            return backfill_budget(current_account, tmp_path, now=NOW, prioritize_due=True,
                                   fixed_incremental_requests=value['reserve_requests'], reservation_plan=value)

        assert budget(original)['allowed'] is False
        assert budget(selected)['allowed'] is True
        assert budget(selected)['reserve'] == 12
        assert worker._next_task(conn, NOW, source_verification_priority=True).data_id == KEY[1]
        assert worker._next_task(conn, NOW, incremental_only=True,
                                 source_verification_priority=True).data_id == 'OLD'
        critical = dict(selected, ready_requests=1, reserve_requests=11)
        assert budget(critical)['allowed'] is False
        assert budget(selected, dict(account, provider_used_in_hour=589))['allowed'] is False
        assert budget(selected, dict(account, observed_at_utc=(NOW-timedelta(minutes=6)).isoformat()))['remaining'] == 0
        assert worker._next_task(conn, NOW, datasets=('USStockPrice',),
                                 source_verification_priority=True).dataset == 'USStockPrice'


@pytest.mark.parametrize('change', ['unregistered', 'changed_plan', 'cooling', 'complete', 'queue_error', 'stale'])
def test_no_finite_priority_without_current_admissible_source_check(tmp_path, change):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        seed(conn); path = plan(tmp_path)
        if change != 'unregistered':
            verification.register(conn, path, NOW)
        accepted_refresh(conn)
        original = reservation([dict(owner='complement', dataset='USStockPrice', requests=1,
                                     due_at_utc=(NOW-timedelta(hours=1)).isoformat())])
        if change == 'changed_plan':
            path.write_text(path.read_text()+'\n')
        elif change == 'cooling':
            conn.execute('UPDATE tasks SET state=?,next_attempt_at_utc=? WHERE dataset=? AND data_id=?',
                         ('failed', (NOW+timedelta(minutes=5)).isoformat(), KEY[0], KEY[1]))
        elif change == 'complete':
            conn.execute('UPDATE tasks SET state=? WHERE dataset=? AND data_id=?', ('complete', *KEY[:2]))
        elif change == 'queue_error':
            original['queue_errors'] = ['sponsor']
        elif change == 'stale':
            original['observed_at_utc'] = (NOW-timedelta(seconds=1)).isoformat()
        assert verification.prioritize_source_checks(conn, original, NOW, frozenset()) is original


def test_canonical_owner_executes_one_exact_check_before_accepted_renewal(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from downloader import finmind_integrity

    account = dict(tier='Free', official_requests_per_hour=600, provider_used_in_hour=0,
                   observed_at_utc=NOW.isoformat())
    source_plan = plan(tmp_path)
    monkeypatch.setenv('FINMIND_TOKEN', 'test-only')
    monkeypatch.setattr(worker, 'load_env_file', lambda *_a, **_kw: None)
    monkeypatch.setattr(worker, '_now', lambda: NOW)
    monkeypatch.setattr(worker, 'verified_account', lambda *_a: account)
    monkeypatch.setattr(worker, 'refresh_dispatch_account', lambda *_a: account)
    monkeypatch.setattr(worker, 'rate_limiter', lambda *_a: object())
    monkeypatch.setattr(worker.shutil, 'disk_usage', lambda *_a: SimpleNamespace(free=10**12))
    monkeypatch.setattr(finmind_integrity, 'audit_completed_batch', lambda *_a: None)

    def populate(conn, *_a, **_kw):
        seed(conn); accepted_refresh(conn)
        verification.register(conn, source_plan, NOW)

    monkeypatch.setattr(worker, '_populate', populate)
    scheduled = reservation([dict(owner='complement', dataset='USStockPrice', requests=1,
                                 due_at_utc=(NOW-timedelta(hours=1)).isoformat())], upcoming=10)
    monkeypatch.setattr(worker, 'incremental_reservation', lambda *_a: scheduled)
    pool_scopes = []

    def pool(*_a, **kwargs):
        pool_scopes.append(kwargs['required_keys'])
        return None

    monkeypatch.setattr(worker.object_pipeline, 'run_batch', pool)
    monkeypatch.setattr(worker.json_pipeline, 'run_batch', pool)
    monkeypatch.setattr(worker, '_claim_bulk_years', lambda *_a, **_kw: pytest.fail('expanded source scope'))
    fetched = []

    def request(_session, _limiter, _root, task, _token, **_kwargs):
        fetched.append((task.dataset, task.data_id, task.partition))
        return []

    monkeypatch.setattr(worker, '_request', request)
    # This test exercises source-priority routing, independently of the
    # machine's real acquisition reserve; capacity admission has its own tests.
    capacity = SimpleNamespace(root=tmp_path.absolute(),
                               initial_sample=lambda: {'state': 'admitted'},
                               sample=lambda: {'state': 'admitted'})
    result = worker.run_once(tmp_path, max_requests=1, capacity_sampler=capacity)
    assert result['state'] == 'batch_complete'
    assert fetched == [KEY] and pool_scopes == [(KEY,), (KEY,)]
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        row = conn.execute('SELECT state,rows,receipt_path,priority FROM tasks '
                           'WHERE dataset=? AND data_id=? AND partition=?', KEY).fetchone()
        assert row[0:2] == ('observed_empty', 0) and row[2] and row[3] == 3
        assert conn.execute("SELECT state FROM tasks WHERE data_id='OLD'").fetchone() == ('complete',)
        assert conn.execute("SELECT state FROM tasks WHERE data_id='TX'").fetchone() == ('pending',)


@pytest.mark.parametrize('due_release', [False, True])
@pytest.mark.parametrize('ordinary_blocked', [False, True])
def test_finite_option_owner_yields_only_foreign_history(tmp_path, monkeypatch, due_release, ordinary_blocked):
    from types import SimpleNamespace
    from downloader import finmind_integrity

    key = ('TaiwanOptionDaily', 'TEO', 'history')
    account = dict(tier='Free', official_requests_per_hour=600, provider_used_in_hour=0,
                   observed_at_utc=NOW.isoformat())
    source_plan = plan(tmp_path, keys=[dict(zip(('dataset', 'data_id', 'partition'), key))])
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [(*key, 'id_history', 3),
                                 ('TaiwanOptionDaily', 'CAO', 'history', 'id_history', 3),
                                 ('TaiwanStockInfo', '', 'latest', 'snapshot', 0)])
        accepted_refresh(conn)
        accepted_refresh(conn, 'EuropeStockPrice', 'EUROPE')
        verification.register(conn, source_plan, NOW)

    monkeypatch.setenv('FINMIND_TOKEN', 'test-only')
    monkeypatch.setattr(worker, 'load_env_file', lambda *_a, **_kw: None)
    monkeypatch.setattr(worker, '_now', lambda: NOW)
    monkeypatch.setattr(worker, 'verified_account', lambda *_a: account)
    monkeypatch.setattr(worker, 'refresh_dispatch_account', lambda *_a: account)
    monkeypatch.setattr(worker, 'rate_limiter', lambda *_a: object())
    monkeypatch.setattr(worker.shutil, 'disk_usage', lambda *_a: SimpleNamespace(free=10**12))
    monkeypatch.setattr(worker, '_populate', lambda *_a, **_kw: pytest.fail('unrelated queue population'))
    monkeypatch.setattr(finmind_integrity, 'audit_completed_batch',
                        lambda *_a, **_kw: pytest.fail('unrelated receipt audit'))
    due = (NOW-timedelta(hours=1)).isoformat()
    events = [dict(owner='complement', dataset='USStockPrice', due_at_utc=due, requests=1),
              dict(owner='complement', dataset='EuropeStockPrice', due_at_utc=due, requests=1)]
    if due_release:
        events.append(dict(owner='complement', dataset='TaiwanStockInfo', due_at_utc=due, requests=1))
    scheduled = reservation(events, upcoming=10)
    monkeypatch.setattr(worker, 'incremental_reservation', lambda *_a: scheduled)
    observed_plans = []
    real_budget = worker.backfill_budget

    def budget(*args, **kwargs):
        observed_plans.append(kwargs['reservation_plan'])
        return real_budget(*args, **kwargs)

    monkeypatch.setattr(worker, 'backfill_budget', budget)
    monkeypatch.setattr(worker.object_pipeline, 'run_batch', lambda *_a, **_kw: None)
    monkeypatch.setattr(worker.json_pipeline, 'run_batch', lambda *_a, **_kw: None)
    fetched, validated = [], []

    def request(_session, _limiter, _root, task, _token, **_kwargs):
        fetched.append((task.dataset, task.data_id, task.partition))
        return [], None

    monkeypatch.setattr(worker, '_request_independent', request)
    from downloader.finmind_option_finite_priority import ORDINARY_CAPACITY_BLOCKED
    sample = {'state': 'admitted', 'sample_age_seconds': 1,
              'guard_kind': ORDINARY_CAPACITY_BLOCKED if ordinary_blocked else 'ordinary_admitted'}
    capacity = SimpleNamespace(root=tmp_path.absolute(),
                               initial_sample=lambda **_kw: sample,
                               sample=lambda: sample)
    result = worker.run_once(tmp_path, max_requests=1, required_keys=(key,),
                             capacity_sampler=capacity, finite_option_priority=True,
                             finite_option_validate=lambda: validated.append(True))
    assert validated and observed_plans
    assert observed_plans[0]['preempted_foreign_history_requests'] == 2
    assert observed_plans[0]['upcoming_requests'] == 10
    assert observed_plans[0]['ready_requests'] == int(due_release)
    blocked = due_release and not ordinary_blocked
    assert result['state'] == ('incremental_reserve' if blocked else 'batch_complete')
    assert fetched == ([] if blocked else [key])
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        assert conn.execute("SELECT state FROM tasks WHERE data_id='OLD'").fetchone() == ('complete',)
        assert conn.execute("SELECT state FROM tasks WHERE data_id='EUROPE'").fetchone() == ('complete',)
        assert conn.execute("SELECT state FROM tasks WHERE data_id='CAO'").fetchone() == ('pending',)
        assert conn.execute("SELECT state FROM tasks WHERE dataset='TaiwanStockInfo'").fetchone() == ('pending',)


def test_finite_option_owner_rejects_unbounded_scope_before_loading_credentials(tmp_path):
    key = ('TaiwanOptionDaily', 'TEO', 'history')
    for kwargs in [dict(required_keys=None, max_requests=1),
                   dict(required_keys=(key,), max_requests=0),
                   dict(required_keys=(key,), max_requests=2),
                   dict(required_keys=(('TaiwanFuturesDaily', 'TX', 'history'),), max_requests=1),
                   dict(required_keys=(key,), datasets=('TaiwanOptionDaily',), max_requests=1)]:
        with pytest.raises(ValueError, match='bounded exact option-product keys'):
            worker.run_once(tmp_path, finite_option_priority=True,
                            finite_option_validate=lambda: None, **kwargs)


@pytest.mark.parametrize('state', ['complete','observed_empty','inflight','blocked','not_entitled','unsupported'])
def test_source_check_does_not_reopen_terminal_empty_or_running_key(tmp_path, state):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        seed(conn)
        conn.execute('UPDATE tasks SET state=? WHERE dataset=? AND data_id=? AND partition=?', (state,*KEY))
        before = conn.execute('SELECT * FROM tasks ORDER BY dataset,data_id').fetchall()
        verification.register(conn, plan(tmp_path), NOW)
        assert worker._next_task(conn, NOW, delegated=frozenset({KEY[0]}), required_keys=(KEY,)) is None
        assert worker._next_task(conn, NOW, delegated=frozenset({KEY[0]})).dataset == 'GoldPrice'
        assert conn.execute('SELECT * FROM tasks ORDER BY dataset,data_id').fetchall() == before


def test_registered_verification_retains_cooldown(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        seed(conn); verification.register(conn, plan(tmp_path), NOW)
        conn.execute('UPDATE tasks SET state=?,next_attempt_at_utc=? WHERE dataset=? AND data_id=? AND partition=?',
            ('failed',(NOW+timedelta(minutes=5)).isoformat(),*KEY))
        assert worker._next_task(conn, NOW, delegated=frozenset({KEY[0]}), required_keys=(KEY,)) is None
        assert worker._next_task(conn, NOW, delegated=frozenset({KEY[0]})).dataset == 'GoldPrice'
        assert worker._next_task(conn, NOW+timedelta(minutes=6), delegated=frozenset({KEY[0]})).data_id == 'FC1'


def test_changed_plan_cannot_authorize_a_new_query(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        seed(conn)
        path = plan(tmp_path); verification.register(conn, path, NOW)
        path.write_text(path.read_text() + '\n')
        assert not verification.permits(conn, KEY)
        assert worker._next_task(conn, NOW, delegated=frozenset({KEY[0]})).dataset == 'GoldPrice'
        with pytest.raises(ValueError, match='previous_product_query_registration'):
            verification.register(conn, path, NOW)


def test_changed_source_pin_rejects_registration_without_task_mutation(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        seed(conn)
        path = plan(tmp_path)
        (tmp_path / 'source-readback.json').write_text('changed')
        before = conn.execute('SELECT * FROM tasks ORDER BY dataset,data_id').fetchall()
        with pytest.raises(ValueError, match='evidence_changed'):
            verification.register(conn, path, NOW)
        assert not conn.execute("SELECT 1 FROM sqlite_master WHERE name=?", (verification.TABLE,)).fetchone()
        assert conn.execute('SELECT * FROM tasks ORDER BY dataset,data_id').fetchall() == before


@pytest.mark.parametrize('changes', [dict(keys=[]), dict(keys=[dict(dataset='TaiwanStockPrice',data_id='2330',partition='history')]),
    dict(keys=[dict(dataset='TaiwanFuturesDaily',data_id='NEVERSEEDED',partition='history')]),
    dict(source_only_no_training_injection=False)])
def test_missing_unbounded_or_other_source_scope_is_rejected(tmp_path, changes):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        seed(conn)
        with pytest.raises(ValueError):
            verification.register(conn, plan(tmp_path, **changes), NOW)
