from datetime import UTC, datetime, timedelta
import json

import pytest

from downloader.artifact_io import sha256_file
from scripts import run_tej_finite_option_axis_capture_wait as waiter


def test_waiter_attaches_only_to_existing_exact_owner_and_reads_back(tmp_path, monkeypatch):
    root = tmp_path / 'owner'
    for name in ('raw', 'requests', 'finite_axis_capture_intents/claimed'):
        (root / name).mkdir(parents=True, exist_ok=True)
    task = 'a' * 24
    attempt = task + '-' + 'b' * 32
    snapshot = root / 'raw' / (attempt + '.json.company-axis.snapshot.json')
    snapshot.write_text('{}')
    candidate = tmp_path / 'candidate.json'
    candidate.write_text('{}')
    (root / 'requests' / (attempt + '.json')).write_text('{}')
    (root / 'worker_status.json').write_text(json.dumps({'state': 'running', 'kind': 'discover',
            'task_id': task, 'bridge_attempt_id': attempt}))
    monkeypatch.setattr(waiter, 'owner_alive', lambda _: True)
    bridge = waiter.REPO / 'scripts/tej_smart_wizard_bridge.ps1'
    monkeypatch.setattr(waiter.DesktopBridge, 'bound_script_release',
                        lambda *a: (bridge, sha256_file(bridge)))
    observed = root / 'raw' / (attempt + '.json.finite-axis.json')
    claim = root / 'finite_axis_capture_intents' / 'claimed' / (attempt + '.json')
    old_claim = claim.parent / (task + '-' + 'e' * 32 + '.json')
    old_claim.write_text('retained earlier attempt')
    calls = []

    def register(*args):
        calls.append(args)
        claim.write_text('finite fixture intent')
        observed.write_text('{}')
        return {'intent_sha256': sha256_file(claim),
                'expires_at_utc': (datetime.now(UTC) + timedelta(minutes=1)).isoformat(), 'labels': 2}

    monkeypatch.setattr(waiter, 'register', register)
    monkeypatch.setattr(waiter, 'audit', lambda *a: {'company_labels': 2, 'training_input_admitted': False})
    output = tmp_path / 'readback.json'
    result = waiter.wait(root, snapshot, candidate, 'c' * 64, datetime(2011, 2, 16).date(), output, 0.01)
    assert result == 0 and len(calls) == 1
    assert json.loads(output.read_text())['training_input_admitted'] is False
    assert old_claim.read_text() == 'retained earlier attempt'


def test_exact_metadata_failure_ends_claimed_wait_without_accepting_data(tmp_path, monkeypatch):
    root = tmp_path / 'owner'
    for name in ('raw', 'requests', 'finite_axis_capture_intents/claimed'):
        (root / name).mkdir(parents=True, exist_ok=True)
    task = 'a' * 24
    attempt = task + '-' + 'b' * 32
    snapshot = root / 'raw' / (attempt + '.json.company-axis.snapshot.json')
    snapshot.write_text('{}')
    candidate = tmp_path / 'candidate.json'
    candidate.write_text('{}')
    request = {'task_id': task, 'action': 'plan', 'type': 'option',
               'smart_id': 'source', 'table': 'history'}
    request_path = root / 'requests' / (attempt + '.json')
    request_path.write_text(json.dumps(request))
    (root / 'worker_status.json').write_text(json.dumps({'state': 'running', 'kind': 'discover',
            'task_id': task, 'bridge_attempt_id': attempt}))
    monkeypatch.setattr(waiter, 'owner_alive', lambda _: True)
    bridge = waiter.REPO / 'scripts/tej_smart_wizard_bridge.ps1'
    monkeypatch.setattr(waiter.DesktopBridge, 'bound_script_release',
                        lambda *a: (bridge, sha256_file(bridge)))
    claim = root / 'finite_axis_capture_intents' / 'claimed' / (attempt + '.json')
    outcome_path = root / 'raw' / (attempt + '.json.outcome.json')
    outcome = {**request, 'contract_version': 4, 'provider': 'tej_smart_wizard',
               'market_data_query_submission_possible': False,
               'error_code': 'metadata_preparation_failed_before_preview'}
    intent = {'contract': 'tej_finite_company_date_axis_capture_once_v1',
              **{k: request[k] for k in ('task_id', 'type', 'smart_id', 'table')},
              'prepared_request_sha256': sha256_file(request_path),
              'bridge_script_sha256': sha256_file(bridge),
              'market_data_query_authorized': False, 'provider_queries_by_registration': 0}

    def register(*args):
        claim.write_text(json.dumps(intent))
        outcome_path.write_text(json.dumps(outcome))
        return {'intent_sha256': sha256_file(claim),
                'expires_at_utc': (datetime.now(UTC) + timedelta(minutes=1)).isoformat(), 'labels': 2}

    monkeypatch.setattr(waiter, 'register', register)
    monkeypatch.setattr(waiter, 'MAX_SAFE_FAILURE_RETRIES', 1)
    output = tmp_path / 'readback.json'
    assert waiter.wait(root, snapshot, candidate, 'c' * 64,
                       datetime(2011, 2, 16).date(), output, 0.01) == 75
    assert not output.exists()
    outcome['table'] = 'different source'
    outcome_path.write_text(json.dumps(outcome))
    with pytest.raises(ValueError, match='exact source attempt'):
        waiter.verified_plan_failure(root, attempt, sha256_file(bridge))


def test_waiter_retries_proved_unsent_failure_on_a_new_owner_attempt(tmp_path, monkeypatch):
    root = tmp_path / 'owner'
    for name in ('raw', 'requests', 'finite_axis_capture_intents/claimed'):
        (root / name).mkdir(parents=True, exist_ok=True)
    task = 'a' * 24
    first = task + '-' + 'b' * 32
    second = task + '-' + 'c' * 32
    snapshot = root / 'raw' / (first + '.json.company-axis.snapshot.json')
    snapshot.write_text('{}')
    candidate = tmp_path / 'candidate.json'
    candidate.write_text('{}')
    native = {'task_id': task, 'action': 'plan', 'type': 'option',
              'smart_id': 'source', 'table': 'history'}
    for attempt in (first, second):
        (root / 'requests' / (attempt + '.json')).write_text(json.dumps(native))
    worker = root / 'worker_status.json'

    def set_worker(attempt):
        worker.write_text(json.dumps({'state': 'running', 'kind': 'discover',
                                      'task_id': task, 'bridge_attempt_id': attempt}))

    set_worker(first)
    monkeypatch.setattr(waiter, 'owner_alive', lambda _: True)
    bridge_sha = sha256_file(waiter.REPO / 'scripts/tej_smart_wizard_bridge.ps1')
    monkeypatch.setattr(waiter.DesktopBridge, 'bound_script_release', lambda *a: (None, bridge_sha))
    calls = []

    def register(*args):
        attempt = json.loads(worker.read_text())['bridge_attempt_id']
        calls.append(attempt)
        request_path = root / 'requests' / (attempt + '.json')
        intent = {'contract': 'tej_finite_company_date_axis_capture_once_v1',
                  **{k: native[k] for k in ('task_id', 'type', 'smart_id', 'table')},
                  'prepared_request_sha256': sha256_file(request_path), 'bridge_script_sha256': bridge_sha,
                  'market_data_query_authorized': False, 'provider_queries_by_registration': 0,
                  'created_at_utc': '2026-10-29T00:00:00Z' if attempt == first else '2026-10-29T00:01:00Z'}
        claim = root / 'finite_axis_capture_intents' / 'claimed' / (attempt + '.json')
        claim.write_text(json.dumps(intent))
        if attempt == first:
            outcome = {**native, 'contract_version': 4, 'provider': 'tej_smart_wizard',
                       'market_data_query_submission_possible': False,
                       'error_code': 'metadata_preparation_failed_before_preview'}
            (root / 'raw' / (attempt + '.json.outcome.json')).write_text(json.dumps(outcome))
        else:
            (root / 'raw' / (attempt + '.json.finite-axis.json')).write_text('{}')
        return {'intent_sha256': sha256_file(claim),
                'expires_at_utc': (datetime.now(UTC) + timedelta(minutes=1)).isoformat(), 'labels': 1}

    monkeypatch.setattr(waiter, 'register', register)
    monkeypatch.setattr(waiter, 'audit', lambda *a: {'company_labels': 1, 'training_input_admitted': False})
    monkeypatch.setattr(waiter.time, 'sleep', lambda _: set_worker(second))
    output = tmp_path / 'readback.json'
    assert waiter.wait(root, snapshot, candidate, 'd' * 64,
                       datetime(2011, 2, 16).date(), output, 0.01) == 0
    assert calls == [first, second]
    assert (root / 'finite_axis_capture_intents/claimed' / (first + '.json')).is_file()
    assert json.loads(output.read_text())['training_input_admitted'] is False


def test_unclaimed_failure_needs_exact_two_source_receipts_before_retirement(tmp_path, monkeypatch):
    root = tmp_path / 'owner'
    for name in ('raw', 'requests', 'finite_axis_capture_intents/claimed'):
        (root / name).mkdir(parents=True, exist_ok=True)
    task = 'a' * 24
    attempt = task + '-' + 'b' * 32
    request = {'task_id': task, 'action': 'plan', 'type': 'option',
               'smart_id': 'source', 'table': 'history'}
    (root / 'requests' / (attempt + '.json')).write_text(json.dumps(request))
    bridge_sha = sha256_file(waiter.REPO / 'scripts/tej_smart_wizard_bridge.ps1')
    monkeypatch.setattr(waiter.DesktopBridge, 'bound_script_release', lambda *a: (None, bridge_sha))
    outcome = {**request, 'contract_version': 4, 'provider': 'tej_smart_wizard',
               'market_data_query_submission_possible': False,
               'error_code': 'metadata_preparation_failed_before_preview'}
    outcome_path = root / 'raw' / (attempt + '.json.outcome.json')
    outcome_path.write_text(json.dumps(outcome))
    intent_path = root / 'finite_axis_capture_intents' / (task + '.json')
    intent = {'contract': 'tej_finite_company_date_axis_capture_once_v1',
              **{k: request[k] for k in ('task_id', 'type', 'smart_id', 'table')},
              'prepared_request_sha256': sha256_file(root / 'requests' / (attempt + '.json')),
              'bridge_script_sha256': bridge_sha, 'market_data_query_authorized': False,
              'provider_queries_by_registration': 0}
    intent_path.write_text(json.dumps(intent))
    digest = sha256_file(intent_path)
    failure_path = root / 'raw' / (attempt + '.json.finite-axis-failure.json')
    failure = {'contract': 'tej_finite_company_date_axis_capture_failure_v1',
               'task_id': task, 'query_attempt_id': attempt, 'intent_claimed': False,
               'market_data_query_submitted': False, 'full_axis_admitted': False}

    # An outcome alone is insufficient to prove that this intent was unclaimed.
    assert waiter.retire_unclaimed_failure(root, intent_path, attempt, digest, bridge_sha) is None
    assert intent_path.is_file()
    for key, value in (('query_attempt_id', task + '-' + 'c' * 32),
                       ('intent_claimed', True), ('market_data_query_submitted', True),
                       ('full_axis_admitted', True)):
        failure_path.write_text(json.dumps({**failure, key: value}))
        with pytest.raises(ValueError, match='unclaimed failure'):
            waiter.retire_unclaimed_failure(root, intent_path, attempt, digest, bridge_sha)
        assert intent_path.is_file()
    failure_path.write_text(json.dumps(failure))
    claim = root / 'finite_axis_capture_intents/claimed' / (attempt + '.json')
    claim.write_bytes(intent_path.read_bytes())
    with pytest.raises(ValueError, match='also has a native claim'):
        waiter.retire_unclaimed_failure(root, intent_path, attempt, digest, bridge_sha)
    assert intent_path.is_file() and claim.is_file()
    claim.unlink()
    retired = waiter.retire_unclaimed_failure(root, intent_path, attempt, digest, bridge_sha)
    assert retired is not None and retired.is_file() and not intent_path.exists()
    assert sha256_file(retired) == digest
    assert failure_path.is_file() and outcome_path.is_file()


def test_unclaimed_failure_retries_only_on_another_existing_attempt(tmp_path, monkeypatch):
    root = tmp_path / 'owner'
    for name in ('raw', 'requests', 'finite_axis_capture_intents/claimed'):
        (root / name).mkdir(parents=True, exist_ok=True)
    task = 'a' * 24
    first = task + '-' + 'b' * 32
    second = task + '-' + 'c' * 32
    snapshot = root / 'raw' / (first + '.json.company-axis.snapshot.json')
    snapshot.write_text('{}')
    candidate = tmp_path / 'candidate.json'
    candidate.write_text('{}')
    request = {'task_id': task, 'action': 'plan', 'type': 'option',
               'smart_id': 'source', 'table': 'history'}
    for attempt in (first, second):
        (root / 'requests' / (attempt + '.json')).write_text(json.dumps(request))
    worker = root / 'worker_status.json'

    def set_worker(attempt):
        worker.write_text(json.dumps({'state': 'running', 'kind': 'discover',
                                      'task_id': task, 'bridge_attempt_id': attempt}))

    set_worker(first)
    monkeypatch.setattr(waiter, 'owner_alive', lambda _: True)
    bridge_sha = sha256_file(waiter.REPO / 'scripts/tej_smart_wizard_bridge.ps1')
    monkeypatch.setattr(waiter.DesktopBridge, 'bound_script_release', lambda *a: (None, bridge_sha))
    calls = []

    def register(*args):
        attempt = json.loads(worker.read_text())['bridge_attempt_id']
        calls.append(attempt)
        intent = {'contract': 'tej_finite_company_date_axis_capture_once_v1',
                  **{k: request[k] for k in ('task_id', 'type', 'smart_id', 'table')},
                  'prepared_request_sha256': sha256_file(root / 'requests' / (attempt + '.json')),
                  'bridge_script_sha256': bridge_sha, 'market_data_query_authorized': False,
                  'provider_queries_by_registration': 0, 'fixture_attempt': attempt}
        intent_path = root / 'finite_axis_capture_intents' / (task + '.json')
        intent_path.write_text(json.dumps(intent))
        digest = sha256_file(intent_path)
        if attempt == first:
            outcome = {**request, 'contract_version': 4, 'provider': 'tej_smart_wizard',
                       'market_data_query_submission_possible': False,
                       'error_code': 'metadata_preparation_failed_before_preview'}
            (root / 'raw' / (attempt + '.json.outcome.json')).write_text(json.dumps(outcome))
            failure = {'contract': 'tej_finite_company_date_axis_capture_failure_v1',
                       'task_id': task, 'query_attempt_id': attempt, 'intent_claimed': False,
                       'market_data_query_submitted': False, 'full_axis_admitted': False}
            (root / 'raw' / (attempt + '.json.finite-axis-failure.json')).write_text(json.dumps(failure))
        else:
            intent_path.rename(root / 'finite_axis_capture_intents/claimed' / (attempt + '.json'))
            (root / 'raw' / (attempt + '.json.finite-axis.json')).write_text('{}')
        return {'intent_sha256': digest,
                'expires_at_utc': (datetime.now(UTC) + timedelta(minutes=1)).isoformat(), 'labels': 1}

    monkeypatch.setattr(waiter, 'register', register)
    monkeypatch.setattr(waiter, 'audit', lambda *a: {'company_labels': 1, 'training_input_admitted': False})
    monkeypatch.setattr(waiter.time, 'sleep', lambda _: set_worker(second))
    output = tmp_path / 'readback.json'
    assert waiter.wait(root, snapshot, candidate, 'd' * 64,
                       datetime(2011, 2, 16).date(), output, 0.01) == 0
    assert calls == [first, second]
    assert len(list((root / 'finite_axis_capture_intents/unclaimed_failed').glob('*.json'))) == 1
    assert json.loads(output.read_text())['training_input_admitted'] is False
