import hashlib
import json
import sqlite3
from datetime import datetime, timezone

import pytest

from scripts.run_finmind_option_product_priority_wait import (
    owner_command, ready_batch, remaining, verified_request,
)
from downloader import finmind_option_finite_priority as finite


def _plans(tmp_path):
    evidence = tmp_path / 'source.csv'
    evidence.write_text('observed option products')
    keys = [{'dataset': 'TaiwanOptionDaily', 'data_id': item, 'partition': 'history'}
            for item in ('TEO', 'CAO')]
    verification = {'contract': 'finmind-finite-product-query-verification-v1',
                    'query_shape': 'per_id_full_history_verification',
                    'authorization': 'user requested source checks', 'reason': 'compare actual data',
                    'source_only_no_training_injection': True, 'keys': keys,
                    'source_evidence': [{'path': str(evidence),
                                         'sha256': hashlib.sha256(evidence.read_bytes()).hexdigest()}]}
    priority = {'contract': 'provider_priority_query_v1', 'provider': 'finmind',
                'created_at_utc': '2026-10-28T00:00:00+00:00',
                'expires_at_utc': '2026-11-03T00:00:00+00:00', 'items': keys}
    vp, pp = tmp_path / 'verification.json', tmp_path / 'priority.json'
    vp.write_text(json.dumps(verification))
    pp.write_text(json.dumps(priority))
    return vp, pp, evidence, verification, priority


def test_bound_product_priority_checks_exact_keys_and_builds_only_owner_commands(tmp_path):
    vp, pp, _, _, _ = _plans(tmp_path)
    request_id, keys, _ = verified_request(vp, pp)
    assert len(request_id) == 64
    assert keys == (('TaiwanOptionDaily', 'TEO', 'history'), ('TaiwanOptionDaily', 'CAO', 'history'))
    command = owner_command(tmp_path, keys)
    assert command[1:3] == ['-m', 'downloader.download_finmind_complement']
    assert command.count('--task-key') == 2
    assert command[command.index('--max-requests') + 1] == '2'
    status = {'provider': 'finmind', 'request_id': request_id, 'items': [
        {'key': json.dumps(keys[0]), 'state': 'complete'},
        {'key': json.dumps(keys[1]), 'state': 'observed_empty'},
    ]}
    assert remaining(status, request_id, keys) == ()  # terminal is not data completeness
    status['items'][1]['state'] = 'pending'
    assert remaining(status, request_id, keys) == (keys[1],)


@pytest.mark.parametrize('defect', ['different_key', 'duplicate_key', 'too_long', 'naive_clock', 'source_changed'])
def test_bound_product_priority_rejects_changed_or_unbounded_inputs(tmp_path, defect):
    vp, pp, evidence, _, priority = _plans(tmp_path)
    if defect == 'different_key':
        priority['items'][0]['data_id'] = 'TXO'
    elif defect == 'duplicate_key':
        priority['items'].append(priority['items'][0])
    elif defect == 'too_long':
        priority['expires_at_utc'] = '2026-11-05T00:00:01+00:00'
    elif defect == 'naive_clock':
        priority['expires_at_utc'] = '2026-11-03T00:00:00'
    else:
        evidence.write_text('different source')
    pp.write_text(json.dumps(priority))
    with pytest.raises(ValueError):
        verified_request(vp, pp)


def test_bound_product_priority_fails_closed_on_bad_receipt_identity_or_keys(tmp_path):
    vp, pp, _, _, _ = _plans(tmp_path)
    request_id, keys, _ = verified_request(vp, pp)
    wrong_id = {'provider': 'finmind', 'request_id': '0' * 64, 'items': []}
    with pytest.raises(ValueError, match='identity'):
        remaining(wrong_id, request_id, keys)
    wrong_keys = {'provider': 'finmind', 'request_id': request_id,
                  'items': [{'key': json.dumps(keys[0]), 'state': 'complete'}]}
    with pytest.raises(ValueError, match='keys differ'):
        remaining(wrong_keys, request_id, keys)


def test_finite_capacity_keeps_exact_plan_registered_keys_and_physical_reserve(tmp_path, monkeypatch):
    vp, pp, _, _, _ = _plans(tmp_path)
    request_id, keys, _ = verified_request(vp, pp)
    with sqlite3.connect(tmp_path / 'queue.sqlite3') as con:
        con.execute('CREATE TABLE tasks(dataset TEXT,data_id TEXT,partition TEXT,kind TEXT)')
        con.executemany('INSERT INTO tasks VALUES (?,?,?,?)', [(*key, 'id_history') for key in keys])
    monkeypatch.setattr(finite, 'inspect', lambda root, request, now: {
        'provider': 'finmind', 'request_id': request, 'priority_active': True})
    monkeypatch.setattr(finite, 'permits', lambda con, key: True)
    seen = []
    monkeypatch.setattr(finite, 'priority_dispatch_status',
                        lambda root, additional_headroom_bytes: seen.append(additional_headroom_bytes)
                        or {'state': 'admitted'})
    assert finite.capacity_probe(tmp_path, vp, pp, keys, 2)['state'] == 'admitted'
    assert seen == [24 * 1024**3 + 6 * 64 * 1024**2]
    command = owner_command(tmp_path, keys, vp, pp)
    assert command[command.index('--option-finite-priority-verification') + 1] == str(vp)
    assert command[command.index('--option-finite-priority-plan') + 1] == str(pp)
    assert request_id


def test_all_registered_products_are_processed_in_bounded_dispatch_batches(tmp_path, monkeypatch):
    vp, pp, _, verification, priority = _plans(tmp_path)
    keys = [{'dataset': 'TaiwanOptionDaily', 'data_id': f'P{i:03d}', 'partition': 'history'}
            for i in range(20)]
    verification['keys'] = priority['items'] = keys
    vp.write_text(json.dumps(verification))
    pp.write_text(json.dumps(priority))
    _, planned, _ = verified_request(vp, pp)
    assert len(planned) == 20
    with sqlite3.connect(tmp_path / 'queue.sqlite3') as con:
        con.execute('CREATE TABLE tasks(dataset TEXT,data_id TEXT,partition TEXT,kind TEXT, '
                    'state TEXT,next_attempt_at_utc TEXT)')
        for i, key in enumerate(planned):
            state = 'complete' if i == 0 else 'failed' if i == 1 else 'pending'
            future = '2026-11-01T00:00:00+00:00' if i == 1 else None
            con.execute('INSERT INTO tasks VALUES (?,?,?,?,?,?)', (*key, 'id_history', state, future))
    now = datetime(2026, 10, 28, tzinfo=timezone.utc)
    batch = ready_batch(tmp_path, planned, now)
    assert batch == planned[2:8]  # complete and cooling items never block later keys
    monkeypatch.setattr(finite, 'inspect', lambda root, request, now: {
        'provider': 'finmind', 'request_id': request, 'priority_active': True})
    monkeypatch.setattr(finite, 'permits', lambda con, key: True)
    finite.validate_owner_request(tmp_path, vp, pp, batch, len(batch), now=now)
    with pytest.raises(ValueError, match='bounded plan keys'):
        finite.validate_owner_request(tmp_path, vp, pp, planned, len(planned), now=now)


def test_finite_probe_marks_only_a_same_observation_physical_capacity_deferral(tmp_path, monkeypatch):
    vp, pp, _, _, _ = _plans(tmp_path)
    _, keys, _ = verified_request(vp, pp)
    monkeypatch.setattr(finite, 'validate_owner_request', lambda *_a, **_kw: None)
    monkeypatch.setattr(finite, 'priority_dispatch_status', lambda *_a, **_kw: {
        'state': 'admitted', 'backing_volume': {'observation_basis': 'same_dispatch_physical_probe'}})
    assert finite.capacity_probe(tmp_path, vp, pp, keys, 2)['guard_kind'] == finite.ORDINARY_CAPACITY_BLOCKED
    monkeypatch.setattr(finite, 'priority_dispatch_status', lambda *_a, **_kw: {'state': 'admitted'})
    assert 'guard_kind' not in finite.capacity_probe(tmp_path, vp, pp, keys, 2)


@pytest.mark.parametrize('defect', ['ordinary_recovered', 'unknown_capacity', 'stale_capacity',
                                    'missing_capacity_age', 'unknown_account', 'insufficient_quota',
                                    'unknown_schedule', 'queue_error', 'no_due_work'])
def test_deferred_priority_never_bypasses_unknown_or_recovered_guards(defect):
    capacity = {'state': 'admitted', 'guard_kind': finite.ORDINARY_CAPACITY_BLOCKED, 'sample_age_seconds': 3}
    budget = {'priority_wait': True, 'schedule_verified': True, 'remaining': 100, 'reserve': 50,
              'basis': 'provider_observation_plus_local_starts'}
    reservation = {'queue_errors': [], 'ready_requests': 48}
    assert finite.capacity_deferred_priority_allowed(capacity, budget, reservation)
    if defect == 'ordinary_recovered': capacity['guard_kind'] = 'ordinary_admitted'
    elif defect == 'unknown_capacity': capacity['state'] = 'not_enrolled'
    elif defect == 'stale_capacity': capacity['sample_age_seconds'] = 31
    elif defect == 'missing_capacity_age': capacity.pop('sample_age_seconds')
    elif defect == 'unknown_account': budget['basis'] = 'provider_usage_unverified'
    elif defect == 'insufficient_quota': budget['remaining'] = budget['reserve']
    elif defect == 'unknown_schedule': budget['schedule_verified'] = False
    elif defect == 'queue_error': reservation['queue_errors'] = ['sponsor']
    else: reservation['ready_requests'] = 0
    assert not finite.capacity_deferred_priority_allowed(capacity, budget, reservation)


@pytest.mark.parametrize('defect', ['unregistered', 'not_active', 'too_many_calls', 'different_key', 'not_enrolled'])
def test_finite_capacity_rejects_unverified_work_and_unknown_backing(tmp_path, monkeypatch, defect):
    vp, pp, _, _, _ = _plans(tmp_path)
    _, keys, _ = verified_request(vp, pp)
    with sqlite3.connect(tmp_path / 'queue.sqlite3') as con:
        con.execute('CREATE TABLE tasks(dataset TEXT,data_id TEXT,partition TEXT,kind TEXT)')
        con.executemany('INSERT INTO tasks VALUES (?,?,?,?)', [(*key, 'id_history') for key in keys])
    monkeypatch.setattr(finite, 'inspect', lambda root, request, now: {
        'provider': 'finmind', 'request_id': request, 'priority_active': defect != 'not_active'})
    monkeypatch.setattr(finite, 'permits', lambda con, key: defect != 'unregistered')
    monkeypatch.setattr(finite, 'priority_dispatch_status',
                        lambda root, additional_headroom_bytes: {'state': 'not_enrolled'})
    if defect == 'not_enrolled':
        assert finite.capacity_probe(tmp_path, vp, pp, keys, 2)['state'] == 'deferred'
        return
    selected = (('TaiwanOptionTick', '', '2026-10-27'),) if defect == 'different_key' else keys
    with pytest.raises(ValueError):
        finite.validate_owner_request(tmp_path, vp, pp, selected, 3 if defect == 'too_many_calls' else len(selected),
                                      now=datetime(2026, 10, 28, tzinfo=timezone.utc))
