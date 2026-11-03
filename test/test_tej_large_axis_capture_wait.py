from datetime import UTC, datetime, timedelta
import json

from scripts import run_tej_large_axis_capture_wait as mod


def test_waiter_arms_only_exact_live_pinned_owner_and_audits(tmp_path, monkeypatch):
    root = tmp_path / 'owner'
    for directory in ('raw', 'requests', 'large_axis_capture_intents/claimed'):
        (root / directory).mkdir(parents=True)
    task = 'a' * 24
    attempt = task + '-' + 'b' * 32
    sample = root / 'raw' / (task + '-' + 'c' * 32 + '.json.company-axis-sample.json')
    sample.write_text('{}')
    (root / 'requests' / (attempt + '.json')).write_text('{}')
    old_claim = root / 'large_axis_capture_intents/claimed' / (task + '-' + 'e' * 32 + '.json')
    old_claim.write_text('retained earlier attempt')
    (root / 'worker_status.json').write_text(json.dumps({'state': 'running', 'kind': 'discover',
                                                        'task_id': task, 'bridge_attempt_id': attempt}))
    monkeypatch.setattr(mod, 'owner_alive', lambda worker: True)
    monkeypatch.setattr(mod.DesktopBridge, 'bound_script_release',
                        lambda *args: (None, mod.sha256_file(mod.REPO / 'scripts/tej_smart_wizard_bridge.ps1')))
    calls = []

    def registration(owner, old_sample, priority):
        calls.append((owner, old_sample, priority))
        intent = owner / 'large_axis_capture_intents' / (task + '.json')
        intent.write_text('{"safe":true}')
        return {'intent_sha256': mod.sha256_file(intent),
                'expires_at_utc': (datetime.now(UTC) + timedelta(minutes=1)).isoformat()}

    def wait_for_claim(_):
        intent = root / 'large_axis_capture_intents' / (task + '.json')
        if intent.exists():
            intent.rename(root / 'large_axis_capture_intents/claimed' / (attempt + '.json'))
            (root / 'raw' / (attempt + '.json.company-axis.snapshot.json')).write_text('{}')

    monkeypatch.setattr(mod, 'register', registration)
    monkeypatch.setattr(mod.time, 'sleep', wait_for_claim)
    monkeypatch.setattr(mod, 'audit', lambda *args: {'source_proof': {'count': 150001},
                                                    'training_input_admitted': False})
    output = tmp_path / 'readback.json'
    assert mod.wait(root, sample, 'd' * 64, output, 1) == 0
    assert len(calls) == 1
    assert json.loads(output.read_text())['training_input_admitted'] is False
    assert old_claim.read_text() == 'retained earlier attempt'


def test_waiter_will_not_arm_a_different_release(tmp_path, monkeypatch):
    root = tmp_path / 'owner'
    for directory in ('raw', 'requests'):
        (root / directory).mkdir(parents=True)
    task = 'a' * 24
    attempt = task + '-' + 'b' * 32
    sample = root / 'raw' / (task + '-' + 'c' * 32 + '.json.company-axis-sample.json')
    sample.write_text('{}')
    (root / 'worker_status.json').write_text(json.dumps({'state': 'running', 'kind': 'discover',
                                                        'task_id': task, 'bridge_attempt_id': attempt}))
    monkeypatch.setattr(mod, 'owner_alive', lambda worker: True)
    monkeypatch.setattr(mod.DesktopBridge, 'bound_script_release', lambda *args: (None, 'f' * 64))
    calls = []
    monkeypatch.setattr(mod, 'register', lambda *args: calls.append(args))
    monkeypatch.setattr(mod.time, 'sleep', lambda _: None)
    assert mod.wait(root, sample, 'd' * 64, tmp_path / 'out.json', 0.000001) == 75
    assert not calls


def test_large_waiter_retries_only_after_attested_unsent_failure(tmp_path, monkeypatch):
    root = tmp_path / 'owner'
    for directory in ('raw', 'requests', 'large_axis_capture_intents/claimed'):
        (root / directory).mkdir(parents=True)
    task = 'a' * 24
    first, second = task + '-' + 'b' * 32, task + '-' + 'c' * 32
    sample = root / 'raw' / (task + '-' + 'd' * 32 + '.json.company-axis-sample.json')
    sample.write_text('{}')
    native = {'task_id': task, 'action': 'plan', 'type': 'option',
              'smart_id': 'source', 'table': 'history'}
    for attempt in (first, second):
        (root / 'requests' / (attempt + '.json')).write_text(json.dumps(native))
    worker = root / 'worker_status.json'

    def set_worker(attempt):
        worker.write_text(json.dumps({'state': 'running', 'kind': 'discover',
                                      'task_id': task, 'bridge_attempt_id': attempt}))

    set_worker(first)
    monkeypatch.setattr(mod, 'owner_alive', lambda _: True)
    bridge_sha = mod.sha256_file(mod.REPO / 'scripts/tej_smart_wizard_bridge.ps1')
    monkeypatch.setattr(mod.DesktopBridge, 'bound_script_release', lambda *a: (None, bridge_sha))
    calls = []

    def register(*args):
        attempt = json.loads(worker.read_text())['bridge_attempt_id']
        calls.append(attempt)
        intent = {'contract': 'tej_large_company_axis_capture_once_v1',
                  **{k: native[k] for k in ('task_id', 'type', 'smart_id', 'table')},
                  'prepared_request_sha256': mod.sha256_file(root / 'requests' / (attempt + '.json')),
                  'bridge_script_sha256': bridge_sha, 'market_data_query_authorized': False,
                  'provider_queries_by_registration': 0,
                  'created_at_utc': '2026-10-29T00:00:00Z' if attempt == first else '2026-10-29T00:01:00Z'}
        claim = root / 'large_axis_capture_intents/claimed' / (attempt + '.json')
        claim.write_text(json.dumps(intent))
        if attempt == first:
            outcome = {**native, 'contract_version': 4, 'provider': 'tej_smart_wizard',
                       'market_data_query_submission_possible': False,
                       'error_code': 'metadata_preparation_failed_before_preview'}
            (root / 'raw' / (attempt + '.json.outcome.json')).write_text(json.dumps(outcome))
        else:
            (root / 'raw' / (attempt + '.json.company-axis.snapshot.json')).write_text('{}')
        return {'intent_sha256': mod.sha256_file(claim),
                'expires_at_utc': (datetime.now(UTC) + timedelta(minutes=1)).isoformat()}

    monkeypatch.setattr(mod, 'register', register)
    monkeypatch.setattr(mod, 'audit', lambda *a: {'source_proof': {'count': 150001},
                                                  'training_input_admitted': False})
    monkeypatch.setattr(mod.time, 'sleep', lambda _: set_worker(second))
    output = tmp_path / 'readback.json'
    assert mod.wait(root, sample, 'e' * 64, output, 0.01) == 0
    assert calls == [first, second]
    assert (root / 'large_axis_capture_intents/claimed' / (first + '.json')).is_file()
    assert json.loads(output.read_text())['training_input_admitted'] is False
