from contextlib import closing
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path

import pytest

from downloader import download_finmind_complement as worker
from downloader.artifact_io import atomic_write_json
from downloader.provider_priority import CONTRACT, finmind_reservation, inspect, register
from downloader.tej_history import _ready_task, connect as wizard_connect
from stockagent.data.finlab_acquisition_contract import safe_stem
from stockagent.data.finlab_gap_priority import pending_gap_requests
from test_tej_planning import registry

NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)


def plan(provider, items):
    return {'contract': CONTRACT, 'provider': provider, 'reason': 'finite research read',
            'created_at_utc': NOW.isoformat(), 'expires_at_utc': (NOW + timedelta(hours=2)).isoformat(),
            'items': items}


def key(dataset, data_id='', partition='2000'):
    return dict(dataset=dataset, data_id=data_id, partition=partition)


def test_finmind_exact_priority_is_idempotent_finite_and_preserves_due_refreshes(tmp_path):
    root = tmp_path / 'complement'
    root.mkdir()
    with worker._db(root / 'queue.sqlite3') as con:
        worker._add_tasks(con, [('GoldPrice', '', '2000', 'year', 3),
                                ('CrudeOilPrices', '', '2000', 'year', 4),
                                ('TaiwanStockInfo', '', 'latest', 'snapshot', 0)])
    request = register(root, plan('finmind', [key('CrudeOilPrices')]), now=NOW)
    assert register(root, plan('finmind', [key('CrudeOilPrices')]), now=NOW) == request
    with worker._db(root / 'queue.sqlite3') as con:
        assert con.execute('SELECT COUNT(*) FROM provider_query_intents').fetchone()[0] == 1
        assert worker._next_task(con, NOW).dataset == 'TaiwanStockInfo'
        assert worker._next_task(con, NOW, background_only=True).dataset == 'CrudeOilPrices'
        con.execute("UPDATE tasks SET state='complete' WHERE priority=0")
        assert worker._next_task(con, NOW).dataset == 'CrudeOilPrices'
        assert worker._next_task(con, NOW, delegated=frozenset({'CrudeOilPrices'})).dataset == 'GoldPrice'
        assert worker._next_task(con, NOW + timedelta(hours=3)).dataset == 'GoldPrice'
        assert worker._next_task(con, NOW, incremental_only=True) is None
        con.execute("UPDATE tasks SET state='failed',next_attempt_at_utc=? WHERE dataset='CrudeOilPrices'",
                    ((NOW + timedelta(days=1)).isoformat(),))
        assert worker._next_task(con, NOW).dataset == 'GoldPrice'
        con.execute("UPDATE tasks SET state='observed_empty' WHERE dataset='CrudeOilPrices'")
        con.execute("UPDATE tasks SET rows=0 WHERE dataset='CrudeOilPrices'")
        assert worker._next_task(con, NOW).dataset == 'GoldPrice'
        assert con.execute("SELECT priority FROM tasks WHERE dataset='CrudeOilPrices'").fetchone()[0] == 4
    readback = inspect(root, request['request_id'], now=NOW)['items'][0]
    assert readback['state'] == 'observed_empty' and readback['rows'] == 0


def test_unknown_batch_is_rejected_atomically(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as con:
        worker._add_tasks(con, [('GoldPrice', '', '2000', 'year', 3)])
    with pytest.raises(ValueError, match='not registered'):
        register(tmp_path, plan('finmind', [key('GoldPrice'), key('NotAProviderDataset')]), now=NOW)
    with worker._db(tmp_path / 'queue.sqlite3') as con:
        assert not con.execute("SELECT 1 FROM sqlite_master WHERE name='provider_query_intents'").fetchone()


def test_finite_read_yields_only_foreign_history_reservations_and_respects_dated_releases(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as con:
        worker._add_tasks(con, [('GoldPrice', '', '2000', 'year', 3),
                                ('USStockPrice', 'AAPL', 'history', 'id_history', 0)])
        con.execute("UPDATE tasks SET next_attempt_at_utc=? WHERE dataset='USStockPrice'", (NOW.isoformat(),))
    register(tmp_path, plan('finmind', [key('GoldPrice')]), now=NOW)
    events = [dict(owner='complement', dataset='USStockPrice', due_at_utc=NOW.isoformat(), requests=1),
              dict(owner='sponsor', dataset='TaiwanStockPrice', due_at_utc=NOW.isoformat(), requests=2)]
    reserve = dict(observed_at_utc=NOW.isoformat(), ready_requests=3, reserve_requests=8,
                   upcoming_requests=5, queue_errors=[], events=events)
    with worker._db(tmp_path / 'queue.sqlite3') as con:
        result = finmind_reservation(con, reserve, NOW, frozenset())
        assert result['ready_requests'] == 2 and result['reserve_requests'] == 7
        assert result['upcoming_requests'] == 5 and result['finite_query_priority']
        assert result['events'][1] == events[1] and reserve['events'][0]['requests'] == 1
        assert worker._next_task(con, NOW).dataset == 'USStockPrice'
        assert worker._next_task(con, NOW, finite_query_priority=True).dataset == 'GoldPrice'
        worker._add_tasks(con, [('TaiwanStockInfo', '', 'latest', 'snapshot', 0)])
        assert worker._next_task(con, NOW, finite_query_priority=True).dataset == 'TaiwanStockInfo'


def test_exact_priority_keys_require_intact_unexpired_registered_request(tmp_path):
    selected = ('TaiwanOptionTick', '', '2025-02-03')
    neighbour = ('TaiwanOptionTick', '', '2025-02-04')
    with worker._db(tmp_path / 'queue.sqlite3') as con:
        worker._add_tasks(con, [(*selected, 'id_day', 1), (*neighbour, 'id_day', 1)])
    request = register(tmp_path, plan('finmind', [key(*selected)]), now=NOW)
    with worker._db(tmp_path / 'queue.sqlite3') as con:
        worker._validate_registered_finite_keys(con, tmp_path, request['request_id'], (selected,), NOW)
        with pytest.raises(ValueError, match='outside'):
            worker._validate_registered_finite_keys(con, tmp_path, request['request_id'], (neighbour,), NOW)
        with pytest.raises(ValueError, match='expired'):
            worker._validate_registered_finite_keys(con, tmp_path, request['request_id'], (selected,), NOW + timedelta(hours=3))
        receipt = tmp_path / 'priority_queries' / (request['request_id'] + '.json')
        payload = json.loads(receipt.read_text())
        payload['plan']['reason'] = 'changed'
        receipt.write_text(json.dumps(payload))
        with pytest.raises(ValueError, match='identity'):
            worker._validate_registered_finite_keys(con, tmp_path, request['request_id'], (selected,), NOW)


@pytest.mark.parametrize('dated_release_due', [False, True])
def test_exact_priority_yields_foreign_history_but_protects_due_release(tmp_path, monkeypatch,
                                                                       dated_release_due):
    from types import SimpleNamespace
    from downloader import finmind_integrity
    from downloader import paused_provider_owners
    monkeypatch.setattr(paused_provider_owners, 'explicitly_paused', lambda _: frozenset())
    selected = ('TaiwanStockDelisting', '', '2025')
    with worker._db(tmp_path / 'queue.sqlite3') as con:
        worker._add_tasks(con, [(*selected, 'year', 4), ('USStockPrice', 'AAPL', 'history', 'id_history', 0)])
        con.execute("UPDATE tasks SET next_attempt_at_utc=? WHERE dataset='USStockPrice'", (NOW.isoformat(),))
    request = register(tmp_path, plan('finmind', [key(*selected)]), now=NOW)
    events = [dict(owner='complement', dataset='USStockPrice', due_at_utc=NOW.isoformat(), requests=1)]
    if dated_release_due:
        events.append(dict(owner='sponsor', dataset='TaiwanStockPrice', due_at_utc=NOW.isoformat(), requests=1))
    reserve = dict(observed_at_utc=NOW.isoformat(), ready_requests=len(events), reserve_requests=len(events),
                   upcoming_requests=0, queue_errors=[], events=events)
    monkeypatch.setenv('FINMIND_TOKEN', 'test-only')
    monkeypatch.setattr(worker, 'load_env_file', lambda *_a, **_k: None)
    monkeypatch.setattr(worker, '_now', lambda: NOW)
    monkeypatch.setattr(worker, 'verified_account', lambda *_a: {'tier': 'Free', 'official_requests_per_hour': 600})
    monkeypatch.setattr(worker, 'rate_limiter', lambda *_a: object())
    monkeypatch.setattr(worker, 'incremental_reservation', lambda *_a: reserve)
    monkeypatch.setattr(worker, 'backfill_budget', lambda *_a, reservation_plan, **_k:
                        {'allowed': reservation_plan['ready_requests'] == 0, 'remaining': 100})
    monkeypatch.setattr(worker.shutil, 'disk_usage', lambda *_a: SimpleNamespace(free=10**12))
    monkeypatch.setattr(finmind_integrity, 'audit_completed_batch', lambda *_a, **_k: pytest.fail('unrelated audit'))
    admitted_capacity = SimpleNamespace(root=tmp_path.absolute(),
                                        initial_sample=lambda **_kwargs: {'state': 'admitted'},
                                        sample=lambda: {'state': 'admitted'})
    fetched = []
    monkeypatch.setattr(worker, '_request', lambda _s, _l, _r, task, _t, **_k:
                        fetched.append((task.dataset, task.data_id, task.partition)) or
                        [{'date': '2025-07-01', 'stock_id': '1234'}])
    result = worker.run_once(tmp_path, max_requests=1, required_keys=(selected,), capacity_sampler=admitted_capacity,
                             finite_priority_request_id=request['request_id'])
    assert result['state'] == ('incremental_reserve' if dated_release_due else 'batch_complete')
    assert fetched == ([] if dated_release_due else [selected])


def test_sponsor_priority_uses_native_release_and_dependency_predicate(tmp_path):
    from downloader import download_finmind_sponsor as sponsor
    with worker._db(tmp_path / 'queue.sqlite3') as con:
        worker._add_tasks(con, [('TaiwanStockPrice', '', '2026-10-26', 'market_day', 1),
                                ('TaiwanStockPER', '', '2026-10-26', 'market_day', 4)])
    register(tmp_path, plan('finmind', [key('TaiwanStockPER', partition='2026-10-26')]), now=NOW)
    with worker._db(tmp_path / 'queue.sqlite3') as con:
        assert sponsor._next(con, NOW).dataset == 'TaiwanStockPER'
        assert con.execute("SELECT state FROM tasks WHERE dataset='TaiwanStockPER'").fetchone()[0] == 'inflight'
        worker._add_tasks(con, [('TaiwanStockInfo', '', 'latest', 'snapshot', 0)])
        assert sponsor._next(con, NOW).dataset == 'TaiwanStockInfo'
        assert con.execute("SELECT state FROM tasks WHERE dataset='TaiwanStockInfo'").fetchone()[0] == 'inflight'
        assert sponsor._next(con, NOW, incremental_only=True) is None  # Already claimed, never resubmit.
        con.execute("UPDATE tasks SET state='failed',next_attempt_at_utc=? WHERE dataset='TaiwanStockPER'",
                    ((NOW + timedelta(days=1)).isoformat(),))
        con.execute("UPDATE tasks SET state='complete' WHERE priority=0")
        assert sponsor._next(con, NOW).dataset == 'TaiwanStockPrice'


def test_teJ_api_priority_preserves_native_retry_and_original_task_order(tmp_path):
    from downloader.tej_api import connect
    from downloader.provider_priority import queued_candidate
    with closing(connect(tmp_path)) as con, con:
        con.executemany("INSERT INTO tasks(id,table_id,params_json,priority,state) VALUES(?,'TRAIL/TAPRCD','{}',?,'pending')",
                        [('history', 0), ('query', 3)])
    request = register(tmp_path, plan('tej_api', ['query']), now=NOW)
    with closing(connect(tmp_path)) as con, con:
        assert queued_candidate(con, NOW.isoformat(), provider='tej_api', where="tasks.state='pending'")['id'] == 'query'
        con.execute("UPDATE tasks SET state='retry',next_attempt_utc=? WHERE id='query'", ((NOW + timedelta(hours=1)).isoformat(),))
        assert queued_candidate(con, NOW.isoformat(), provider='tej_api', where="tasks.state IN ('pending','retry') AND (next_attempt_utc IS NULL OR next_attempt_utc<=?)", args=(NOW.isoformat(),)) is None
        assert con.execute("SELECT priority FROM tasks WHERE id='query'").fetchone()[0] == 3
    assert request['provider_calls'] == 0


@pytest.mark.parametrize('defect', ['expired', 'future', 'too_long', 'no_reason'])
def test_unbounded_or_ambiguous_requests_are_rejected(tmp_path, defect):
    p = plan('finlab', ['price:close'])
    if defect == 'expired': p['expires_at_utc'] = NOW.isoformat()
    if defect == 'future': p['created_at_utc'] = (NOW + timedelta(seconds=1)).isoformat()
    if defect == 'too_long': p['expires_at_utc'] = (NOW + timedelta(days=8)).isoformat()
    if defect == 'no_reason': p['reason'] = ''
    with pytest.raises(ValueError): register(tmp_path, p, now=NOW)


def test_repeated_and_reordered_scopes_share_one_canonical_request(tmp_path):
    from downloader.tej_api import connect
    with closing(connect(tmp_path)) as con, con:
        con.executemany("INSERT INTO tasks(id,table_id,params_json,priority,state) "
                        "VALUES(?,'TRAIL/TAPRCD','{}',3,'pending')", [('a',), ('b',)])
    first = register(tmp_path, plan('tej_api', ['b', 'a', 'b']), now=NOW)
    second = register(tmp_path, plan('tej_api', ['a', 'b']), now=NOW)
    assert first == second and len(first['items']) == 2 and first['provider_calls'] == 0
    with closing(connect(tmp_path)) as con:
        assert con.execute('SELECT COUNT(*) FROM provider_query_intents').fetchone()[0] == 2
        assert con.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone()[0] == 2


def test_prior_raw_plan_receipt_remains_idempotent_after_canonicalization(tmp_path):
    import hashlib
    from downloader.tej_api import connect
    with closing(connect(tmp_path)) as con, con:
        con.executemany("INSERT INTO tasks(id,table_id,params_json,priority,state) "
                        "VALUES(?,'TRAIL/TAPRCD','{}',3,'pending')", [('a',), ('b',)])
    original = plan('tej_api', ['b', 'a'])
    digest = hashlib.sha256(json.dumps(original, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    atomic_write_json(tmp_path / 'priority_queries' / (digest + '.json'),
                      {'contract': CONTRACT, 'request_id': digest, 'plan': original, 'provider_calls': 0})
    result = register(tmp_path, original, now=NOW)
    assert result['request_id'] == digest and len(result['items']) == 2
    assert len(list((tmp_path / 'priority_queries').glob('*.json'))) == 1


def test_large_finmind_demand_has_no_request_or_active_registry_cap(tmp_path, monkeypatch):
    import sqlite3
    from downloader import provider_priority as priority
    count = 4105
    items = [key('TaiwanStockPrice', f'S{i:05d}', 'history') for i in range(count)]
    with worker._db(tmp_path / 'queue.sqlite3') as con:
        worker._add_tasks(con, [('TaiwanStockPrice', item['data_id'], 'history', 'id_history', 3)
                                for item in items])
    original_connect = priority._connect
    queries = []
    def connect(root):
        con = original_connect(root)
        con.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999)
        con.set_trace_callback(queries.append)
        return con
    monkeypatch.setattr(priority, '_connect', connect)
    result = register(tmp_path, plan('finmind', items + items[:70]), now=NOW)
    assert len(result['items']) == count and result['provider_calls'] == 0
    # Registration and status use SQL batches, rather than one lookup per key.
    lookups = [q for q in queries if q.startswith('WITH requested(')]
    assert 1 < len(lookups) < count // 10
    overlap = {**plan('finmind', items[-100:]), 'reason': 'another reader, same native tasks'}
    register(tmp_path, overlap, now=NOW)
    with worker._db(tmp_path / 'queue.sqlite3') as con:
        assert len(priority.active_keys(con, NOW)) == count
        assert con.execute('SELECT COUNT(*) FROM provider_query_intents').fetchone()[0] == count + 100
        assert con.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone()[0] == count


def test_large_finlab_cli_plan_and_owner_registry_are_not_silently_capped(tmp_path, monkeypatch, capsys):
    import sys
    from scripts import request_provider_data as cli
    items = ['example:' + 'x' * 390 + f'{i:05d}' for i in range(6000)]
    atomic_write_json(tmp_path / 'catalog/discovery.json', {'keys': items})
    monkeypatch.setattr('scripts.download_finlab_history.has_local_download', lambda *_: False)
    # Use the real CLI clock rather than expiring this fixture as time moves.
    stamp = datetime.now(UTC)
    request = {**plan('finlab', items), 'created_at_utc': stamp.isoformat(),
               'expires_at_utc': (stamp + timedelta(hours=1)).isoformat()}
    path = tmp_path / 'large-plan.json'; atomic_write_json(path, request)
    assert path.stat().st_size > 1024**2
    monkeypatch.setattr(sys, 'argv', ['request_provider_data.py', 'submit', '--plan', str(path), '--root', str(tmp_path)])
    cli.main()
    result = json.loads(capsys.readouterr().out)
    assert len(result['items']) == len(items) and result['provider_calls'] == 0
    assert (tmp_path / 'gap_priority_requests.json').stat().st_size > 4 * 1024**2
    assert len(pending_gap_requests(tmp_path, now=stamp)) == len(items)
    monkeypatch.setattr(sys, 'argv', ['request_provider_data.py', 'list', '--provider', 'finlab',
                                    '--root', str(tmp_path), '--limit', '0'])
    cli.main()
    assert len(json.loads(capsys.readouterr().out)['items']) == len(items)


def test_large_intraday_demand_keeps_tail_visible_to_the_original_owner(tmp_path):
    from downloader.provider_priority import finlab_stock_candidates
    from scripts.download_finlab_market_intraday import _open_index
    db = tmp_path / 'intraday/market_index.sqlite3'
    db.parent.mkdir()
    _open_index(db).close()
    days = [date(2014, 1, 1) + timedelta(days=i) for i in range(4105)]
    items = [dict(dataset='tw_minute:2330', partition=day.isoformat()) for day in days]
    result = register(tmp_path, plan('finlab', items), now=NOW)
    assert len(result['items']) == len(days) and result['provider_calls'] == 0
    import sqlite3
    with closing(sqlite3.connect(db)) as con:
        candidates = finlab_stock_candidates(con, NOW, 'tw_minute')
    assert candidates == [('tw_minute:2330', day) for day in days]


def test_unknown_item_in_later_sql_batch_rolls_back_the_entire_registration(tmp_path, monkeypatch):
    import sqlite3
    from downloader import provider_priority as priority
    items = [key('TaiwanStockPrice', f'S{i:05d}', 'history') for i in range(1100)]
    with worker._db(tmp_path / 'queue.sqlite3') as con:
        worker._add_tasks(con, [('TaiwanStockPrice', item['data_id'], 'history', 'id_history', 3)
                                for item in items])
    original_connect = priority._connect
    def connect(root):
        con = original_connect(root)
        con.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999)
        return con
    monkeypatch.setattr(priority, '_connect', connect)
    with pytest.raises(ValueError, match='not registered'):
        register(tmp_path, plan('finmind', items + [key('TaiwanStockPrice', 'ZZ_UNKNOWN', 'history')]), now=NOW)
    with worker._db(tmp_path / 'queue.sqlite3') as con:
        assert not priority.has_intents(con)
        assert con.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone()[0] == len(items)
    assert not list((tmp_path / 'priority_queries').glob('*.json'))


def test_overlapping_finlab_requests_read_each_native_receipt_once(tmp_path, monkeypatch):
    atomic_write_json(tmp_path / 'catalog/discovery.json', {'keys': ['price:close']})
    first = plan('finlab', ['price:close'])
    monkeypatch.setattr('scripts.download_finlab_history.has_local_download', lambda *_: False)
    register(tmp_path, first, now=NOW)
    register(tmp_path, {**first, 'reason': 'second reader'}, now=NOW)
    path = tmp_path / 'receipts' / (safe_stem('price:close') + '.json')
    atomic_write_json(path, {'dataset': 'price:close', 'source_checked_at_utc': NOW.isoformat(),
                            'source_check_mode': 'upstream_incremental'})
    reads = []
    original = Path.read_text
    def read(p, *args, **kwargs):
        if p == path: reads.append(p)
        return original(p, *args, **kwargs)
    monkeypatch.setattr(Path, 'read_text', read)
    assert not pending_gap_requests(tmp_path, now=NOW)
    assert reads == [path]


def test_finlab_uses_same_gap_intent_and_true_source_clock_without_empty_fill_claim(tmp_path):
    atomic_write_json(tmp_path / 'catalog/discovery.json', {'keys': ['price:close', 'tw_minute:2330']})
    result = register(tmp_path, plan('finlab', ['price:close']), now=NOW)
    assert pending_gap_requests(tmp_path, now=NOW)['price:close']['reason'] == 'interactive_query'
    path = tmp_path / 'receipts' / (safe_stem('price:close') + '.json')
    native = {'dataset': 'price:close', 'source_checked_at_utc': NOW.isoformat(),
              'source_check_mode': 'sdk_cache_allowed'}
    atomic_write_json(path, native)
    assert inspect(tmp_path, result['request_id'], now=NOW)['items'][0]['state'] == 'pending_existing_owner'
    native.update(source_check_mode='upstream_incremental', last_check_result='unchanged')
    atomic_write_json(path, native)
    assert not pending_gap_requests(tmp_path, now=NOW)
    assert inspect(tmp_path, result['request_id'], now=NOW)['items'][0]['state'] == 'upstream_checked'
    assert inspect(tmp_path, result['request_id'], now=NOW + timedelta(days=1))['items'][0]['state'] == 'upstream_checked'
    with pytest.raises(ValueError, match='intraday'):
        register(tmp_path, plan('finlab', ['tw_minute:2330']), now=NOW)
    assert not result['data_completeness_claim'] and result['provider_calls'] == 0


def test_finlab_local_cache_read_does_not_force_an_upstream_download(tmp_path, monkeypatch):
    atomic_write_json(tmp_path / 'catalog/discovery.json', {'keys': ['price:close']})
    monkeypatch.setattr('scripts.download_finlab_history.has_local_download', lambda *args: True)
    result = register(tmp_path, plan('finlab', ['price:close']), now=NOW)
    assert result['items'][0]['state'] == 'available_locally'
    assert not pending_gap_requests(tmp_path, now=NOW)
    fresh = {**plan('finlab', ['price:close']), 'fresh_after_utc': NOW.isoformat()}
    result = register(tmp_path, fresh, now=NOW)
    assert result['items'][0]['state'] == 'pending_existing_owner'
    assert 'price:close' in pending_gap_requests(tmp_path, now=NOW)


def test_finlab_futures_priority_uses_existing_partition_index_and_native_receipts(tmp_path):
    from downloader.provider_priority import finlab_futures_candidates
    from scripts.download_finlab_market_intraday import _open_index
    from scripts.download_finlab_intraday import _partition_paths

    root = tmp_path / 'finlab'
    root.mkdir()
    db = root / 'intraday/market_index.sqlite3'
    db.parent.mkdir()
    with closing(_open_index(db)):
        pass
    item = {'dataset': 'tw_futures_minute:I5F_201611', 'partition': '2016-11-08'}
    request = register(root, plan('finlab', [item]), now=NOW)
    assert request['items'][0]['state'] == 'pending_existing_owner'
    assert register(root, plan('finlab', [item]), now=NOW) == request
    with closing(_open_index(db)) as con:
        assert finlab_futures_candidates(con, NOW) == [('tw_futures_minute:I5F_201611', date(2016, 11, 8))]
        assert not finlab_futures_candidates(con, NOW + timedelta(hours=3))
    receipt, attempt = _partition_paths(root, item['dataset'], date.fromisoformat(item['partition']))
    atomic_write_json(attempt, {'dataset': item['dataset'], 'trade_date': item['partition'],
        'status': 'partition_not_ready', 'attempted_at_utc': NOW.isoformat(), 'provider_code': 'not_ready'})
    readback = inspect(root, request['request_id'], now=NOW)['items'][0]
    assert readback['state'] == 'waiting_retry_existing_owner' and readback['last_attempt_state'] == 'partition_not_ready'
    atomic_write_json(receipt, {'dataset': item['dataset'], 'trade_date': item['partition'],
        'status': 'verified_no_trade', 'rows': 0})
    assert inspect(root, request['request_id'], now=NOW)['items'][0]['state'] == 'verified_no_trade'
    with pytest.raises(ValueError, match='cannot mix'):
        register(root, plan('finlab', [item, 'price:close']), now=NOW)
    with pytest.raises(ValueError, match='future'):
        register(root, plan('finlab', [{**item, 'partition': '2027-01-01'}]), now=NOW)


def test_exact_futures_request_preference_reorders_without_removing_other_intents(tmp_path):
    from downloader.provider_priority import finlab_futures_candidates
    from scripts.download_finlab_market_intraday import _open_index
    root = tmp_path / 'finlab'
    db = root / 'intraday/market_index.sqlite3'
    db.parent.mkdir(parents=True)
    with closing(_open_index(db)):
        pass
    old = {'dataset': 'tw_futures_minute:AAA_201611', 'partition': '2016-11-08'}
    new = {'dataset': 'tw_futures_minute:ZZZ_202610', 'partition': '2026-10-02'}
    first = register(root, plan('finlab', [old]), now=NOW)
    second = register(root, plan('finlab', [new]), now=NOW)
    with closing(_open_index(db)) as con:
        before = finlab_futures_candidates(con, NOW)
        assert before == [(old['dataset'], date.fromisoformat(old['partition'])),
                          (new['dataset'], date.fromisoformat(new['partition']))]
        after = finlab_futures_candidates(con, NOW, prefer_request_ids=(second['request_id'],))
        assert after == [before[1], before[0]]
        assert len(after) == len(before) and set(after) == set(before)
        assert not finlab_futures_candidates(con, NOW + timedelta(hours=3),
                                             prefer_request_ids=(second['request_id'],))
        with pytest.raises(ValueError, match='unique exact request'):
            finlab_futures_candidates(con, NOW, prefer_request_ids=('invalid',))
    assert inspect(root, first['request_id'], now=NOW)['items'][0]['state'] == 'pending_existing_owner'
    assert inspect(root, second['request_id'], now=NOW)['items'][0]['state'] == 'pending_existing_owner'


def test_finlab_exact_stock_priorities_reuse_native_partition_receipts(tmp_path):
    from downloader.provider_priority import finlab_stock_candidates
    from scripts.download_finlab_market_intraday import _open_index
    from scripts.download_finlab_intraday import _partition_paths

    root = tmp_path / 'finlab'
    db = root / 'intraday/market_index.sqlite3'
    db.parent.mkdir(parents=True)
    with closing(_open_index(db)):
        pass
    item = {'dataset': 'tw_tick:5203', 'partition': '2011-10-03'}
    request = register(root, plan('finlab', [item]), now=NOW)
    assert request['items'][0]['state'] == 'pending_existing_owner'
    assert register(root, plan('finlab', [item]), now=NOW) == request
    with closing(_open_index(db)) as con:
        assert finlab_stock_candidates(con, NOW, 'tw_tick') == [('tw_tick:5203', date(2011, 10, 3))]
        assert finlab_stock_candidates(con, NOW, 'tw_minute') == []
        assert finlab_stock_candidates(con, NOW + timedelta(hours=3), 'tw_tick') == []
    receipt, attempt = _partition_paths(root, item['dataset'], date(2011, 10, 3))
    atomic_write_json(attempt, {'status': 'partition_not_ready', 'attempted_at_utc': NOW.isoformat()})
    assert inspect(root, request['request_id'], now=NOW)['items'][0]['state'] == 'waiting_retry_existing_owner'
    atomic_write_json(receipt, {'dataset': item['dataset'], 'trade_date': item['partition'],
                               'status': 'verified_no_trade', 'rows': 0})
    assert inspect(root, request['request_id'], now=NOW)['items'][0]['state'] == 'verified_no_trade'
    for invalid in ('tw_tick:../5203', 'tw_tick:5203!', 'tw_quote:5203'):
        with pytest.raises(ValueError):
            register(root, plan('finlab', [{**item, 'dataset': invalid}]), now=NOW)
    with pytest.raises(ValueError, match='cannot mix'):
        register(root, plan('finlab', [item, 'price:close']), now=NOW)
    with pytest.raises(ValueError, match='future'):
        register(root, plan('finlab', [{**item, 'partition': '2027-01-01'}]), now=NOW)


def test_finlab_futures_priority_rejects_same_size_tampered_cached_object(tmp_path):
    import hashlib
    from scripts.download_finlab_market_intraday import _open_index
    from scripts.download_finlab_intraday import _partition_paths

    root = tmp_path / 'finlab'
    db = root / 'intraday/market_index.sqlite3'
    db.parent.mkdir(parents=True)
    with closing(_open_index(db)):
        pass
    item = {'dataset': 'tw_futures_minute:TX_202610', 'partition': '2026-10-02'}
    obj = root / 'intraday/objects/example.parquet'
    obj.parent.mkdir()
    obj.write_bytes(b'good')
    receipt, _ = _partition_paths(root, item['dataset'], date.fromisoformat(item['partition']))
    atomic_write_json(receipt, {'dataset': item['dataset'], 'trade_date': item['partition'],
        'status': 'downloaded_unverified_for_pit', 'parquet_path': str(obj.relative_to(root)),
        'parquet_size_bytes': 4, 'sha256': hashlib.sha256(b'good').hexdigest()})
    assert register(root, plan('finlab', [item]), now=NOW)['items'][0]['state'] == 'downloaded_unverified_for_pit'
    obj.write_bytes(b'evil')
    first = register(root, plan('finlab', [item]), now=NOW)
    assert inspect(root, first['request_id'], now=NOW)['items'][0]['state'] == 'invalid_local_receipt'
    fresh_plan = {**plan('finlab', [item]), 'reason': 'recheck corrupted receipt'}
    assert register(root, fresh_plan, now=NOW)['items'][0]['state'] == 'invalid_local_receipt'
    from downloader.provider_priority import finlab_futures_candidates
    with closing(_open_index(db)) as con:
        assert finlab_futures_candidates(con, NOW) == [('tw_futures_minute:TX_202610', date(2026, 10, 2))]


def test_api_native_split_retains_finite_priority_and_parent_receipt(tmp_path):
    from types import SimpleNamespace
    from downloader.tej_api import connect
    from downloader.download_tej_api import _partition_at_cap
    with closing(connect(tmp_path)) as con, con:
        params = json.dumps({'mdate.gte': '2026-10-01', 'mdate.lte': '2026-10-27'})
        con.execute("INSERT INTO tasks(id,table_id,params_json,priority,state) VALUES('parent','TRAIL/TAPRCD',?,4,'pending')", (params,))
    result = register(tmp_path, plan('tej_api', ['parent']), now=NOW)
    with closing(connect(tmp_path)) as con, con:
        parent = dict(con.execute("SELECT * FROM tasks WHERE id='parent'").fetchone())
        state = _partition_at_cap(con, parent, {}, SimpleNamespace(root=tmp_path))
        con.execute("UPDATE tasks SET state=? WHERE id='parent'", (state,))
        assert con.execute('SELECT COUNT(*) FROM provider_query_intents').fetchone()[0] == 3
    current = inspect(tmp_path, result['request_id'], now=NOW)
    assert current['items'][0]['state'] == 'superseded_disjoint_partition'
    assert sum(row.get('derived_from_requested_scope', False) for row in current['items']) == 2


def test_wizard_native_selector_split_retains_finite_priority(registry):
    from test_tej_selector_geometry import pending_scope
    from downloader.tej_planning import repartition_selector_pending
    root, _ = registry
    parent, _ = pending_scope(root, days=10000)
    result = register(root, plan('tej', [parent['task_id']]), now=NOW)
    with closing(wizard_connect(root)) as con, con:
        assert repartition_selector_pending(con, root, parent)
        assert con.execute('SELECT COUNT(*) FROM provider_query_intents').fetchone()[0] == 6
    current = inspect(root, result['request_id'], now=NOW)
    assert sum(row.get('derived_from_requested_scope', False) for row in current['items']) == 5


def test_wizard_priority_does_not_bypass_table_quarantine_or_due_incrementals(tmp_path):
    root = tmp_path / 'tej'
    with closing(wizard_connect(root)) as con, con:
        for task_id, table_id, priority in [('history', 'a', 0), ('interactive', 'b', 200), ('due', 'c', -1000)]:
            con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,scope_contract) "
                        "VALUES(?,?,'download','P1',?,'{}','editable_source_scope_v1')", (task_id, table_id, priority))
    register(root, plan('tej', ['interactive']), now=NOW)
    with closing(wizard_connect(root)) as con, con:
        assert _ready_task(con, NOW.isoformat(), None, {})['task_id'] == 'due'
        con.execute("UPDATE tasks SET state='complete' WHERE task_id='due'")
        assert _ready_task(con, NOW.isoformat(), None, {})['task_id'] == 'interactive'
        con.execute("UPDATE tasks SET state='blocked',last_error_code='source_validation_failed_deferred' WHERE task_id='interactive'")
        assert _ready_task(con, NOW.isoformat(), None, {})['task_id'] == 'history'


def test_request_registry_never_reopens_completed_or_unknown_tasks(tmp_path):
    root = tmp_path / 'tej'
    with closing(wizard_connect(root)) as con, con:
        for identity, state in [('done', 'complete'), ('uncertain', 'blocked')]:
            con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,state,request_json,scope_contract) "
                        "VALUES(?,'t','download','P1',100,?,'{}','editable_source_scope_v1')", (identity, state))
    request = register(root, plan('tej', ['done', 'uncertain']), now=NOW)
    assert [x['state'] for x in request['items']] == ['complete', 'blocked']
    with closing(wizard_connect(root)) as con:
        assert _ready_task(con, NOW.isoformat(), None, {}) is None
