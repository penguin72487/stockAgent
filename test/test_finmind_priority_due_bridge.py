from datetime import UTC, datetime, timedelta
import json

import pytest

from scripts import run_finmind_priority_due_bridge as bridge


NOW = datetime(2026, 10, 29, 7, tzinfo=UTC)


def event(owner, dataset, delta=0):
    return {'owner': owner, 'dataset': dataset, 'requests': 1,
            'due_at_utc': (NOW + timedelta(seconds=delta)).isoformat()}


def test_due_scopes_only_selects_verified_already_due_sponsor_work():
    plan = {'observed_at_utc': NOW.isoformat(), 'queue_errors': [], 'events': [
        event('sponsor', 'TaiwanStockBlockTrade'),
        event('sponsor', 'TaiwanStockDividend'),
        event('sponsor', 'TaiwanFuturesDaily', 1),
        event('complement', 'TaiwanStockPrice'),
    ]}
    assert bridge.due_scopes(plan, NOW) == ('release_preconditions', 'underlying_research')
    assert bridge.due_scopes({**plan, 'queue_errors': ['unknown']}, NOW) == ()
    assert bridge.due_scopes({**plan, 'observed_at_utc': None}, NOW) == ()


def test_bridge_uses_existing_owner_only_while_registered_priority_is_pending(tmp_path, monkeypatch):
    root = tmp_path / 'complement'
    root.mkdir()
    (root / 'queue.sqlite3').touch()
    (tmp_path / 'sponsor').mkdir()
    (tmp_path / 'sponsor' / 'queue.sqlite3').touch()
    calls = []
    done = [False]
    monkeypatch.setattr(bridge, 'inspect', lambda _root, rid, now: {
        'provider': 'finmind', 'request_id': rid, 'priority_active': True,
        'items': [{'state': 'complete' if done[0] else 'pending'}],
    })
    monkeypatch.setattr(bridge, 'incremental_reservation', lambda _root, now: {
        'observed_at_utc': now.isoformat(), 'queue_errors': [],
        'events': [event('sponsor', 'TaiwanStockBlockTrade')],
    })
    def owner(path, **kwargs):
        calls.append((path, kwargs))
        done[0] = True
        return {'state': 'batch_complete'}
    monkeypatch.setattr(bridge, 'sponsor_run', owner)
    result = bridge.run(root, request_ids=('exact-request',), max_wait_seconds=60,
                        clock=lambda: NOW, monotonic=lambda: 0, sleep=lambda _seconds: None)
    assert result['state'] == 'no_active_dispatchable_requests'
    assert result['provider_calls_by_bridge'] == 0
    assert len(calls) == 1 and calls[0][0] == tmp_path / 'sponsor'
    assert calls[0][1]['datasets'] == bridge.SCOPES['release_preconditions']
    assert calls[0][1]['max_requests'] == 16


def test_bridge_rejects_duplicate_or_unverified_requests(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match='unique'):
        bridge.run(tmp_path, request_ids=('a', 'a'), max_wait_seconds=60)
    root = tmp_path / 'complement'
    root.mkdir()
    (root / 'queue.sqlite3').touch()
    (tmp_path / 'sponsor').mkdir()
    (tmp_path / 'sponsor' / 'queue.sqlite3').touch()
    monkeypatch.setattr(bridge, 'inspect', lambda *_a, **_k: {'provider': 'other', 'request_id': 'a'})
    with pytest.raises(ValueError, match='identity'):
        bridge.run(root, request_ids=('a',), max_wait_seconds=60, clock=lambda: NOW)


def write_batch(prefix, number, request_id, key):
    path = prefix.with_name(f'{prefix.name}_batch_{number:04d}.json')
    plan = {'contract': bridge.BACKLOG_CONTRACT,
            'scope': 'per_product_physical_day_source_comparison_only_no_training_injection',
            'checks': [{'dataset': key[0], 'data_id': key[1], 'partition': key[2]}]}
    path.write_text(json.dumps(plan))
    receipt = {'contract': bridge.BACKLOG_CONTRACT, 'source_plan': str(path), 'request_id': request_id}
    path.with_name(path.stem + '_registration.json').write_text(json.dumps(receipt))
    return path


def test_backlog_follows_new_registered_batches_with_the_same_native_owner(tmp_path, monkeypatch):
    monkeypatch.setattr(bridge, 'REPO', tmp_path)
    root = tmp_path / 'complement'
    root.mkdir()
    (root / 'queue.sqlite3').touch()
    (tmp_path / 'sponsor').mkdir()
    (tmp_path / 'sponsor' / 'queue.sqlite3').touch()
    artifacts = tmp_path / 'artifacts' / 'markets'
    artifacts.mkdir(parents=True)
    prefix = artifacts / 'known_backlog'
    first, second = 'a' * 64, 'b' * 64
    key1 = ('TaiwanOptionTick', 'TXO', '2026-10-21')
    key2 = ('TaiwanOptionTick', 'TEO', '2026-10-22')
    write_batch(prefix, 32, first, key1)
    states = {first: ('pending', key1), second: ('pending', key2)}
    calls, elapsed = [], [0]
    def inspect(_root, rid, now):
        state, key = states.get(rid, ('complete', key1))
        return {'provider': 'finmind', 'request_id': rid, 'priority_active': True,
                'items': [{'state': state, 'key': json.dumps(key)}]}
    monkeypatch.setattr(bridge, 'inspect', inspect)
    monkeypatch.setattr(bridge, 'incremental_reservation', lambda _root, now: {
        'observed_at_utc': now.isoformat(), 'queue_errors': [],
        'events': [event('sponsor', 'TaiwanStockBlockTrade')],
    })
    def owner(path, **kwargs):
        calls.append(path)
        if len(calls) == 1:
            states[first] = ('complete', key1)
            write_batch(prefix, 33, second, key2)
        else:
            states[second] = ('complete', key2)
        return {'state': 'batch_complete'}
    monkeypatch.setattr(bridge, 'sponsor_run', owner)
    result = bridge.run(root, request_ids=('direct',), backlog_prefix=prefix, max_wait_seconds=65,
                        clock=lambda: NOW, monotonic=lambda: elapsed[0],
                        sleep=lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds))
    assert len(calls) == 2
    assert result['state'] == 'finite_wait_elapsed'
    assert result['provider_calls_by_bridge'] == 0


def test_backlog_rejects_mismatched_registration_and_outside_prefix(tmp_path, monkeypatch):
    monkeypatch.setattr(bridge, 'REPO', tmp_path)
    artifacts = tmp_path / 'artifacts' / 'markets'
    artifacts.mkdir(parents=True)
    prefix = artifacts / 'backlog'
    key = ('TaiwanOptionTick', 'TXO', '2026-10-21')
    path = write_batch(prefix, 1, 'a' * 64, key)
    registration = path.with_name(path.stem + '_registration.json')
    receipt = json.loads(registration.read_text())
    receipt['source_plan'] = str(artifacts / 'other.json')
    registration.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match='registration'):
        bridge.backlog_request(prefix)
    with pytest.raises(ValueError, match='artifact root'):
        bridge.backlog_request(tmp_path / 'elsewhere')


def test_bridge_stops_when_the_matching_backlog_workflow_ends(tmp_path, monkeypatch):
    monkeypatch.setattr(bridge, 'REPO', tmp_path)
    root = tmp_path / 'complement'
    root.mkdir()
    (root / 'queue.sqlite3').touch()
    (tmp_path / 'sponsor').mkdir()
    (tmp_path / 'sponsor' / 'queue.sqlite3').touch()
    prefix = tmp_path / 'artifacts' / 'markets' / 'backlog'
    prefix.parent.mkdir(parents=True)
    record = tmp_path / 'artifacts' / 'operations' / 'agent-workflow' / 'runs' / 'batch' / 'run.json'
    record.parent.mkdir(parents=True)
    doc = {'repo_root': str(tmp_path), 'state': 'succeeded',
           'argv': ['run_fintech_python', 'scripts/run_finmind_option_corrected_product_backlog.py',
                    '--output-prefix', str(prefix)]}
    record.write_text(json.dumps(doc))
    monkeypatch.setattr(bridge, 'inspect', lambda _root, rid, now: {
        'provider': 'finmind', 'request_id': rid, 'priority_active': True,
        'items': [{'state': 'complete'}],
    })
    result = bridge.run(root, request_ids=('direct',), backlog_prefix=prefix,
                        backlog_run_record=record, max_wait_seconds=7 * 86400,
                        clock=lambda: NOW, monotonic=lambda: 0, sleep=lambda _: None)
    assert result['state'] == 'backlog_run_ended'
    assert result['backlog_run_state'] == 'succeeded'
    doc['argv'][-1] = str(tmp_path / 'different')
    record.write_text(json.dumps(doc))
    with pytest.raises(ValueError, match='identity'):
        bridge.backlog_run_state(record, prefix)
