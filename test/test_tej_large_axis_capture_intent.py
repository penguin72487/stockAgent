from datetime import UTC, datetime, timedelta
import hashlib
import json
import sqlite3

import pytest

from scripts import register_tej_large_axis_capture as mod
from downloader.tej_history import CONTRACT_VERSION, DESKTOP_INPUT_CONTRACT


@pytest.fixture
def setup_intent(tmp_path, monkeypatch):
    root = tmp_path / 'owner'
    for name in ('raw', 'requests', 'launches'):
        (root / name).mkdir(parents=True)
    task = 'a' * 24
    attempt = task + '-' + 'b' * 32
    req = {'action': 'plan', 'type': 'Option (ALL)', 'smart_id': 'TEJ Derivatives',
           'table': 'Delisted Option(2010~2013)', 'fields': ['price']}
    prepared = {**req, 'contract_version': CONTRACT_VERSION, 'task_id': task,
                'desktop_input_contract': DESKTOP_INPUT_CONTRACT}
    request_file = root / 'requests' / (attempt + '.json')
    request_file.write_text(json.dumps(prepared))
    sample_file = root / 'raw' / (attempt + '.json.company-axis-sample.json')
    sample = {'contract': 'large_company_axis_diagnostic_only_v1', 'task_id': task,
              'prepared_request_sha256': hashlib.sha256(request_file.read_bytes()).hexdigest(),
              'market_data_query_submitted': False, 'full_axis_admitted': False,
              'source_scope_proof': {'binding_readback_verified': True, 'vendor_notices_absent': True},
              'available_count': 510703, **{k: req[k] for k in ('type', 'smart_id', 'table')}}
    sample_file.write_text(json.dumps(sample))
    now = datetime.now(UTC)
    with sqlite3.connect(root / 'queue.sqlite3') as con:
        con.execute('CREATE TABLE tasks(task_id TEXT, table_id TEXT, kind TEXT, state TEXT, request_json TEXT)')
        con.execute('CREATE TABLE provider_query_intents(request_id TEXT, task_key TEXT, '
                    'created_at_utc TEXT, expires_at_utc TEXT)')
        con.execute('INSERT INTO tasks VALUES(?,?,?,?,?)', (task, 'c' * 24, 'discover', 'pending', json.dumps(req)))
        con.execute('INSERT INTO provider_query_intents VALUES(?,?,?,?)',
                    ('d' * 64, task, (now - timedelta(days=1)).isoformat(), (now + timedelta(days=1)).isoformat()))
    monkeypatch.setattr(mod.DesktopBridge, 'bound_script_release', lambda *args: (None, 'e' * 64))
    monkeypatch.setattr(mod, 'inspect', lambda *args: {'provider': 'tej', 'priority_active': True,
                                                     'expires_at_utc': (now + timedelta(days=1)).isoformat()})
    monkeypatch.setattr(mod, 'priority_dispatch_status', lambda *args, **kwargs: {
        'state': 'admitted', 'available_bytes': 1000_000_000, 'required_bytes': 100_000_000,
        'backing_volume': {'physical_free_bytes': 1000_000_000, 'required_bytes': 100_000_000}})
    return root, sample_file, task


def test_one_exact_intent_is_created_without_query(setup_intent):
    root, sample, task = setup_intent
    result = mod.register(root, sample, 'd' * 64)
    body = json.loads((root / 'large_axis_capture_intents' / (task + '.json')).read_text())
    assert result['source_calls'] == 0 and result['source_data_admitted'] is False
    assert body['market_data_query_authorized'] is False and body['expected_count'] == 510703
    with pytest.raises(FileExistsError):
        mod.register(root, sample, 'd' * 64)


def test_rejects_changed_source_and_low_physical_space(setup_intent, monkeypatch):
    root, sample, task = setup_intent
    body = json.loads(sample.read_text())
    body['table'] = 'different'
    sample.write_text(json.dumps(body))
    with pytest.raises(ValueError, match='binding differs'):
        mod.register(root, sample, 'd' * 64)
    body['table'] = 'Delisted Option(2010~2013)'
    sample.write_text(json.dumps(body))
    monkeypatch.setattr(mod, 'priority_dispatch_status', lambda *args, **kwargs: {'state': 'deferred'})
    with pytest.raises(ValueError, match='headroom'):
        mod.register(root, sample, 'd' * 64)
    assert not (root / 'large_axis_capture_intents' / (task + '.json')).exists()


def test_rejects_already_claimed_task(setup_intent):
    root, sample, task = setup_intent
    claim = root / 'large_axis_capture_intents' / 'claimed'
    claim.mkdir(parents=True)
    (claim / (task + '-' + 'f' * 32 + '.json')).write_text('{}')
    with pytest.raises(ValueError, match='unknown'):
        mod.register(root, sample, 'd' * 64)


def test_registration_preserves_attested_unsent_claim(setup_intent, monkeypatch):
    root, sample, task = setup_intent
    previous = task + '-' + 'f' * 32
    original = root / 'requests' / (task + '-' + 'b' * 32 + '.json')
    previous_request = root / 'requests' / (previous + '.json')
    previous_request.write_bytes(original.read_bytes())
    request = json.loads(previous_request.read_text())
    bridge_sha = 'e' * 64
    intent = {'contract': mod.INTENT, **{k: request[k] for k in ('task_id', 'type', 'smart_id', 'table')},
              'prepared_request_sha256': hashlib.sha256(previous_request.read_bytes()).hexdigest(),
              'bridge_script_sha256': bridge_sha, 'market_data_query_authorized': False,
              'provider_queries_by_registration': 0}
    claim = root / 'large_axis_capture_intents' / 'claimed' / (previous + '.json')
    claim.parent.mkdir(parents=True)
    claim.write_text(json.dumps(intent))
    outcome = {**{k: request[k] for k in ('task_id', 'type', 'smart_id', 'table', 'action')},
               'contract_version': CONTRACT_VERSION, 'provider': 'tej_smart_wizard',
               'market_data_query_submission_possible': False,
               'error_code': 'metadata_preparation_failed_before_preview'}
    outcome_path = root / 'raw' / (previous + '.json.outcome.json')
    outcome_path.write_text(json.dumps(outcome))
    result = mod.register(root, sample, 'd' * 64)
    assert result['source_calls'] == 0 and claim.is_file() and outcome_path.is_file()
    assert (root / 'large_axis_capture_intents' / (task + '.json')).is_file()


def test_large_capture_uses_priority_buffer_and_requires_full_physical_headroom(setup_intent, monkeypatch):
    root, sample, task = setup_intent
    seen = []

    def capacity(path, *, additional_headroom_bytes):
        seen.append((path, additional_headroom_bytes))
        return {'state': 'admitted', 'available_bytes': 1000_000_000, 'required_bytes': 100_000_000,
                'backing_volume': {'physical_free_bytes': 100_000_000 + mod.MAX_CAPTURE_BYTES - 1,
                                   'required_bytes': 100_000_000}}

    monkeypatch.setattr(mod, 'priority_dispatch_status', capacity)
    with pytest.raises(ValueError, match='headroom'):
        mod.register(root, sample, 'd' * 64)
    assert seen == [(root.resolve(), 24 * 1024 ** 3)]
    assert not (root / 'large_axis_capture_intents' / (task + '.json')).exists()
