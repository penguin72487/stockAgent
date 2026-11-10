import json
from datetime import UTC, datetime, timedelta

import pytest

from scripts import run_finlab_finite_priority_handoff as handoff


def _status(identifier, now, expiry, *, state='pending_existing_owner', dataset='tw_futures_minute:TX_201611'):
    active = now < expiry
    return {'contract': 'provider_priority_query_v1', 'provider': 'finlab', 'request_id': identifier,
            'provider_calls': 0, 'priority_active': active, 'expires_at_utc': expiry.isoformat(),
            'items': [{'key': json.dumps([dataset, '2016-11-07']),
                       'state': state if active else 'expired'}]}


def test_request_scope_and_invalid_receipts_fail_closed():
    now = datetime(2026, 10, 29, tzinfo=UTC)
    expiry = now + timedelta(days=1)
    identifier = 'a' * 64
    read = lambda root, name, now: _status(name, now, expiry)
    summary = handoff.request_summary([identifier], now, reader=read)
    assert summary['active_waiting'] == 1
    assert summary['latest_active_expiry'] == expiry
    with pytest.raises(ValueError, match='explicit futures'):
        handoff.request_summary([identifier], now, reader=lambda root, name, now:
                                _status(name, now, expiry, dataset='tw_minute:2330'))
    with pytest.raises(ValueError, match='needs review'):
        handoff.request_summary([identifier], now, reader=lambda root, name, now:
                                _status(name, now, expiry, state='invalid_local_receipt'))


@pytest.mark.parametrize('dataset,partition,accepted', [
    ('tw_minute:3059', '2026-11-09', True),
    ('tw_minute:00707R', '2026-11-09', True),
    ('tw_minute:', '2026-11-09', False),
    ('tw_minute:3059/other', '2026-11-09', False),
    ('tw_minute:3059', '2026-02-30', False),
    ('tw_tick:3059', '2026-11-09', False),
    ('tw_futures_minute:TX_202611', '2026-11-09', False),
])
def test_stock_handoff_only_accepts_explicit_valid_stock_minute_scope(dataset, partition, accepted):
    now = datetime(2026, 11, 9, tzinfo=UTC)
    identifier, expiry = 'a' * 64, now + timedelta(days=1)
    def reader(root, name, now):
        result = _status(name, now, expiry, dataset=dataset)
        result['items'][0]['key'] = json.dumps([dataset, partition])
        return result
    if accepted:
        assert handoff.request_summary([identifier], now, reader=reader, family='stock')['active_waiting'] == 1
    else:
        with pytest.raises(ValueError):
            handoff.request_summary([identifier], now, reader=reader, family='stock')


def test_predecessor_identity_handles_pid_reuse_and_zombie(tmp_path):
    proc = tmp_path / '123'
    proc.mkdir()
    stat = proc / 'stat'
    stat.write_text('123 (name with spaces) ' + ' '.join(['S'] + ['0'] * 18 + ['42']))
    assert handoff.process_start_ticks(123, proc_root=tmp_path) == 42
    stat.write_text('123 (name) ' + ' '.join(['Z'] + ['0'] * 18 + ['42']))
    assert handoff.process_start_ticks(123, proc_root=tmp_path) is None
    assert handoff.process_start_ticks(456, proc_root=tmp_path) is None


def test_handoff_waits_for_predecessor_then_uses_canonical_owner(tmp_path, monkeypatch):
    now = [datetime(2026, 10, 29, tzinfo=UTC)]
    expiry = now[0] + timedelta(hours=1)
    identifier = 'a' * 64
    state = ['pending_existing_owner']
    events = []
    pin = tmp_path / 'pin'
    pin.write_text('stable')
    monkeypatch.setattr(handoff, 'OWNER_FILES', (pin,))

    def sleep(seconds):
        events.append('sleep')
        now[0] += timedelta(seconds=seconds)

    def process(pid):
        return 42 if not events else None

    def reader(root, name, now):
        return _status(name, now, expiry, state=state[0])

    def owner(passes, *, kind, capacity_wait):
        events.append('owner')
        assert passes == 32 and kind == 'tw_minute' and capacity_wait()
        state[0] = 'downloaded_unverified_for_pit'
        return 0, {'state': 'no_more_actionable_in_pass'}

    progress = tmp_path / 'progress.json'
    code = handoff.run([identifier], 123, 42, progress, now=lambda: now[0], sleep=sleep,
                       reader=reader, process=process, drain_owner=owner,
                       headroom=lambda deadline, *, still_valid: still_valid())
    assert code == 0 and events == ['sleep', 'owner']
    doc = json.loads(progress.read_text())
    assert doc['stage'] == 'requests_expired_or_no_longer_waiting'
    assert doc['data_completeness_claim'] is False
    assert doc['states'] == {'downloaded_unverified_for_pit': 1}


def test_stock_handoff_waits_for_the_existing_owner_and_uses_same_guarded_drain(tmp_path, monkeypatch):
    now = [datetime(2026, 11, 9, tzinfo=UTC)]
    expiry, identifier = now[0] + timedelta(hours=1), 'b' * 64
    state, events = ['pending_existing_owner'], []
    pin = tmp_path / 'pin'
    pin.write_text('stable')
    monkeypatch.setattr(handoff, 'OWNER_FILES', (pin,))
    def sleep(seconds):
        events.append('wait_for_existing_owner')
        now[0] += timedelta(seconds=seconds)
    def reader(root, name, now):
        return _status(name, now, expiry, state=state[0], dataset='tw_minute:3059')
    def owner(passes, *, kind, capacity_wait):
        events.append('canonical_owner')
        assert passes == 32 and kind == 'tw_minute' and capacity_wait()
        state[0] = 'downloaded_unverified_for_pit'
        return 0, {'state': 'no_more_actionable_in_pass'}
    progress = tmp_path / 'progress.json'
    result = handoff.run([identifier], 123, 42, progress, now=lambda: now[0], sleep=sleep,
        reader=reader, process=lambda pid: 42 if not events else None, drain_owner=owner,
        headroom=lambda deadline, *, still_valid: still_valid(), family='stock')
    assert result == 0 and events == ['wait_for_existing_owner', 'canonical_owner']
    payload = json.loads(progress.read_text())
    assert payload['family'] == 'stock' and payload['data_completeness_claim'] is False
    with pytest.raises(ValueError, match='family'):
        handoff.run([identifier], 123, 42, progress, family='stock', futures_workspace_only=True)


def test_workspace_handoff_pins_storage_and_passes_only_the_workspace_mode(tmp_path, monkeypatch):
    now = datetime(2026, 11, 3, tzinfo=UTC)
    expiry = now + timedelta(hours=1)
    state = ['pending_existing_owner']
    pin, storage_pin = tmp_path / 'owner', tmp_path / 'workspace'
    pin.write_text('stable')
    storage_pin.write_text('stable-volume')
    monkeypatch.setattr(handoff, 'OWNER_FILES', (pin,))
    monkeypatch.setattr(handoff, 'WORKSPACE_FILES', (storage_pin,))
    def headroom(deadline, *, still_valid, sample):
        assert still_valid() and callable(sample)
        return True
    def owner(passes, *, kind, capacity_wait, futures_workspace_only):
        assert futures_workspace_only and capacity_wait()
        state[0] = 'downloaded_unverified_for_pit'
        return 0, {'state': 'no_more_actionable_in_pass'}
    progress = tmp_path / 'progress.json'
    code = handoff.run(['a' * 64], 123, 42, progress, now=lambda: now,
        reader=lambda root, name, now: _status(name, now, expiry, state=state[0]),
        process=lambda pid: None, drain_owner=owner, headroom=headroom, futures_workspace_only=True)
    result = json.loads(progress.read_text())
    assert code == 0 and result['futures_workspace_only'] is True
    assert str(storage_pin) in result['source_sha256']


def test_workspace_handoff_passes_only_registered_preferred_requests(tmp_path, monkeypatch):
    now = datetime(2026, 11, 3, tzinfo=UTC)
    expiry = now + timedelta(hours=1)
    pin = tmp_path / 'owner'
    pin.write_text('stable')
    monkeypatch.setattr(handoff, 'OWNER_FILES', (pin,))
    monkeypatch.setattr(handoff, 'WORKSPACE_FILES', ())
    requested, preferred = 'a'*64, 'b'*64
    state = ['pending_existing_owner']
    def owner(passes, *, kind, capacity_wait, futures_workspace_only, preferred_futures_request_ids):
        assert futures_workspace_only and preferred_futures_request_ids == (preferred,)
        assert capacity_wait()
        state[0] = 'downloaded_unverified_for_pit'
        return 0, {'state': 'no_more_actionable_in_pass'}
    code = handoff.run([requested, preferred], 123, 42, tmp_path/'progress.json', now=lambda: now,
        reader=lambda root, name, now: _status(name, now, expiry, state=state[0]),
        process=lambda pid: None, drain_owner=owner,
        headroom=lambda deadline, *, still_valid, sample: still_valid(), futures_workspace_only=True,
        preferred_futures_request_ids=(preferred,))
    assert code == 0
    with pytest.raises(ValueError, match='unique registered'):
        handoff.run([requested], 123, 42, tmp_path/'other.json', futures_workspace_only=True,
                    preferred_futures_request_ids=(preferred,))


def test_capacity_wait_cannot_authorize_a_request_after_expiry(tmp_path, monkeypatch):
    now = [datetime(2026, 10, 29, tzinfo=UTC)]
    expiry = now[0] + timedelta(minutes=10)
    pin = tmp_path / 'pin'
    pin.write_text('stable')
    monkeypatch.setattr(handoff, 'OWNER_FILES', (pin,))

    def reader(root, name, now):
        return _status(name, now, expiry)

    def headroom(deadline, *, still_valid):
        now[0] = expiry
        return True

    def owner(passes, *, kind, capacity_wait):
        assert not capacity_wait()
        return 75, {'state': 'capacity_wait_timeout'}

    progress = tmp_path / 'progress.json'
    assert handoff.run(['a' * 64], 123, 42, progress, now=lambda: now[0], reader=reader,
                       process=lambda pid: None, drain_owner=owner, headroom=headroom) == 0
    assert json.loads(progress.read_text())['states'] == {'expired': 1}


def test_changed_owner_stops_without_dispatch(tmp_path, monkeypatch):
    now = datetime(2026, 10, 29, tzinfo=UTC)
    expiry = now + timedelta(hours=1)
    pin = tmp_path / 'pin'
    pin.write_text('stable')
    monkeypatch.setattr(handoff, 'OWNER_FILES', (pin,))
    values = iter(['old', 'new'])
    progress = tmp_path / 'progress.json'
    assert handoff.run(['a' * 64], 123, 42, progress, now=lambda: now,
                       reader=lambda root, name, now: _status(name, now, expiry),
                       process=lambda pid: None, digest=lambda path: next(values),
                       drain_owner=lambda *a, **kw: pytest.fail('must not dispatch')) == 75
    assert json.loads(progress.read_text())['stage'] == 'owner_changed_review_required'


def test_changed_owner_while_waiting_for_capacity_never_dispatches(tmp_path, monkeypatch):
    now = datetime(2026, 11, 3, tzinfo=UTC)
    expiry = now + timedelta(hours=2)
    pin = tmp_path / 'pin'
    pin.write_text('old')
    monkeypatch.setattr(handoff, 'OWNER_FILES', (pin,))

    def headroom(deadline, *, still_valid):
        assert still_valid()
        pin.write_text('new')
        assert not still_valid()
        return False

    def owner(passes, *, kind, capacity_wait):
        assert not capacity_wait()
        return 75, {'state': 'capacity_wait_timeout'}

    progress = tmp_path / 'progress.json'
    assert handoff.run(['a' * 64], 123, 42, progress, now=lambda: now,
                       reader=lambda root, name, now: _status(name, now, expiry),
                       process=lambda pid: None, drain_owner=owner, headroom=headroom) == 75
    assert json.loads(progress.read_text())['stage'] == 'owner_changed_review_required'


def test_only_known_deferrals_are_retried():
    now = datetime(2026, 10, 29, 23, 59, tzinfo=UTC)
    assert handoff.retry_delay(75, {'state': 'child_deferred', 'child_state': 'quota_margin_reached'}, now) == 120
    assert handoff.retry_delay(75, {'state': 'capacity_wait_timeout'}, now) == 0
    assert handoff.retry_delay(75, {'state': 'child_deferred', 'child_state': 'priority_handoff'}, now) == 300
    assert handoff.retry_delay(75, {'state': 'child_deferred', 'child_state': 'unknown'}, now) is None
    assert handoff.retry_delay(1, {'state': 'pass_limit_reached'}, now) is None


@pytest.mark.parametrize('completes', [True, False])
def test_daytrade_handoff_retries_through_owner_and_stops_at_request_expiry(tmp_path, monkeypatch, completes):
    now = [datetime(2026, 11, 2, tzinfo=UTC)]
    expiry = now[0] + timedelta(minutes=6)
    pin = tmp_path / 'pin'
    pin.write_text('stable')
    monkeypatch.setattr(handoff, 'OWNER_FILES', (pin,))
    state = ['pending_existing_owner']
    calls, sleeps = [], []

    def owner(passes, *, kind, capacity_wait):
        assert now[0] < expiry and capacity_wait()
        calls.append((passes, kind))
        if completes and len(calls) == 2:
            state[0] = 'downloaded_unverified_for_pit'
            return 0, {'state': 'no_more_actionable_in_pass'}
        return 75, {'state': 'child_deferred', 'child_state': 'priority_handoff'}

    def sleep(seconds):
        sleeps.append(seconds)
        now[0] += timedelta(seconds=seconds)

    progress = tmp_path / 'progress.json'
    code = handoff.run(['a' * 64], 123, 42, progress, now=lambda: now[0],
                       reader=lambda root, name, now: _status(name, now, expiry, state=state[0]),
                       process=lambda pid: None, drain_owner=owner, sleep=sleep,
                       headroom=lambda deadline, *, still_valid: still_valid())
    assert code == 0 and calls == [(32, 'tw_minute')] * 2
    assert sleeps == ([300] if completes else [300, 60])
    result = json.loads(progress.read_text())
    assert result['stage'] == 'requests_expired_or_no_longer_waiting'
    assert result['states'] == {('downloaded_unverified_for_pit' if completes else 'expired'): 1}
    assert result['data_completeness_claim'] is False
