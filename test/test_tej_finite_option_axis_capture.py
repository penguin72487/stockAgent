from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import sqlite3

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from downloader.tej_history import DesktopBridge
from scripts import audit_tej_finite_option_axis_capture as auditor
from scripts import register_tej_finite_option_axis_capture as registrar


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding='utf-8')


@pytest.fixture
def candidate(tmp_path, monkeypatch):
    monkeypatch.setattr(registrar, 'REPO', tmp_path)
    monkeypatch.setattr(registrar, 'iter_snapshot', lambda _: iter(['A', 'B', 'C']))
    data_root = tmp_path / 'data_tw_index_options_daily'
    data_root.mkdir()
    binary = tmp_path / 'axis.bin'
    binary.write_bytes(b'native fixture')
    implementation = tmp_path / 'implementation.py'
    implementation.write_text('verified', encoding='utf-8')
    data = data_root / 'candidate.parquet'
    pl.DataFrame({'date': [date(2011, 2, 16), date(2011, 2, 16), date(2014, 12, 11)],
                  'candidate_labels': [['A'], ['C'], ['B']], 'candidate_count': [1, 1, 1],
                  'status': ['unique_label_candidate'] * 3}).write_parquet(data)
    manifest = {'contract': 'tej_option_official_gap_menu_candidates_v1',
                'source_table': 'Delisted Option(2010~2013)', 'provider_queries': 0,
                'market_observations_obtained': False, 'full_axis_admitted_to_plan': False,
                'training_ready': False, 'data_path': str(data), 'data_sha256': sha256_file(data),
                'source_pins': {str(binary): sha256_file(binary)},
                'implementation_sha256': {str(implementation): sha256_file(implementation)}}
    path = data_root / 'manifest.json'
    _write(path, manifest)
    snapshot = {'table': manifest['source_table'], 'snapshot_path': str(binary),
                'source_proof': {'sha256': sha256_file(binary)}}
    return path, manifest, snapshot, implementation


def test_finite_candidates_respect_date_source_and_fingerprints(candidate):
    path, manifest, snapshot, implementation = candidate
    labels, digest = registrar.candidate_labels(path, snapshot, date(2011, 2, 16))
    assert labels == ['A', 'C'] and digest == sha256_file(path)
    with pytest.raises(ValueError, match='outside this table'):
        registrar.candidate_labels(path, snapshot, date(2014, 12, 11))
    implementation.write_text('changed', encoding='utf-8')
    with pytest.raises(ValueError, match='implementation fingerprint'):
        registrar.candidate_labels(path, snapshot, date(2011, 2, 16))


def test_ambiguous_or_missing_menu_candidate_is_not_admitted(candidate, monkeypatch):
    path, manifest, snapshot, _ = candidate
    monkeypatch.setattr(registrar, 'iter_snapshot', lambda _: iter(['A', 'B']))
    with pytest.raises(ValueError, match='absent from the retained native menu'):
        registrar.candidate_labels(path, snapshot, date(2011, 2, 16))
    data = Path(manifest['data_path'])
    pl.DataFrame({'date': [date(2011, 2, 16)], 'candidate_labels': [['A', 'C']],
                  'candidate_count': [2], 'status': ['ambiguous']}).write_parquet(data)
    manifest['data_sha256'] = sha256_file(data)
    _write(path, manifest)
    with pytest.raises(ValueError, match='Ambiguous'):
        registrar.candidate_labels(path, snapshot, date(2011, 2, 16))


@pytest.fixture
def captured(tmp_path, monkeypatch):
    root = tmp_path / 'data_tej'
    task = 'a' * 24
    attempt = task + '-' + 'b' * 32
    release_sha = 'c' * 64
    snapshot_sha = 'd' * 64
    candidate_sha = 'e' * 64
    request = {'action': 'plan', 'frequency': 'daily', 'task_id': task,
               'type': 'Option (ALL)', 'smart_id': 'TEJ Derivatives',
               'table': 'Delisted Option(2010~2013)', 'fields': ['price', 'volume'],
               'start': '2011-01-01', 'end': '2011-12-31'}
    request_path = root / 'requests' / (attempt + '.json')
    _write(request_path, request)
    snapshot_manifest = root / 'raw' / 'snapshot.json'
    _write(snapshot_manifest, {'synthetic': True})
    binary = root / 'raw' / 'snapshot.bin'
    binary.write_bytes(b'not a vendor snapshot')
    intent = {'contract': registrar.INTENT, 'task_id': task, 'priority_request_id': 'f' * 64,
              'type': request['type'], 'smart_id': request['smart_id'], 'table': request['table'],
              'prepared_request_sha256': sha256_file(request_path), 'bridge_script_sha256': release_sha,
              'market_data_query_authorized': False, 'provider_queries_by_registration': 0,
              'created_at_utc': '2026-10-29T00:00:00+00:00',
              'expires_at_utc': '2026-10-29T00:12:00+00:00',
              'snapshot_manifest_path': str(snapshot_manifest),
              'snapshot_manifest_sha256': sha256_file(snapshot_manifest),
              'prior_snapshot_sha256': snapshot_sha, 'candidate_manifest_path': 'unused',
              'candidate_manifest_sha256': candidate_sha, 'expected_count': 3,
              'company_labels': ['A', 'C'], 'date': '2011-02-16'}
    claim = root / 'finite_axis_capture_intents' / 'claimed' / (attempt + '.json')
    _write(claim, intent)
    scope = {'binding_readback_verified': True, 'vendor_notices_absent': True}
    obs = {'contract': 'tej_finite_company_date_axis_observation_v1',
           'task_id': task, 'query_attempt_id': attempt,
           'type': request['type'], 'smart_id': request['smart_id'], 'table': request['table'],
           'prepared_request_sha256': sha256_file(request_path), 'bridge_script_sha256': release_sha,
           'intent_sha256': sha256_file(claim), 'priority_request_id': intent['priority_request_id'],
           'prior_snapshot_sha256': snapshot_sha, 'candidate_manifest_sha256': candidate_sha,
           'available_company_count': 3, 'company_labels': ['A', 'C'], 'company_indices': [0, 2],
           'date_search_start': intent['date'], 'date_search_end': intent['date'],
           'date_labels': ['2011/02/16'], 'fields': request['fields'],
           'before_scope': scope, 'after_scope': scope, 'complete_live_company_reads': 2,
           'complete_date_reads': 2, 'source_key_mode': 2, 'observed_at_utc': '2026-10-29T00:05:00Z',
           'market_data_query_submitted': False, 'full_axis_admitted': False}
    path = root / 'raw' / (attempt + '.json.finite-axis.json')
    _write(path, obs)
    snapshot = {'task_id': task, 'table': request['table'], 'snapshot_path': str(binary),
                'source_proof': {'sha256': snapshot_sha, 'count': 3}}
    monkeypatch.setattr(auditor.DesktopBridge, 'bound_script_release', lambda *a: (None, release_sha))
    monkeypatch.setattr(auditor, 'audit_snapshot', lambda *a: snapshot)
    monkeypatch.setattr(auditor, 'candidate_labels', lambda *a: (['A', 'C'], candidate_sha))
    monkeypatch.setattr(auditor, 'iter_snapshot', lambda _: iter(['A', 'B', 'C']))
    return root, path, obs


def test_independent_witness_stays_research_only(captured):
    root, path, _ = captured
    proof = auditor.audit(root, path)
    assert proof['company_labels'] == 2 and proof['date_menu_contains_day'] is True
    assert proof['market_observations_obtained'] is False
    assert proof['training_input_admitted'] is False


@pytest.mark.parametrize(('key', 'value'), [
    ('market_data_query_submitted', True), ('full_axis_admitted', True),
    ('complete_live_company_reads', 1), ('fields', ['different']),
    ('source_key_mode', 1),
    ('date_labels', ['2011/02/17']), ('company_indices', [2, 0]),
    ('date_search_start', '2011-02-15'), ('intent_sha256', '0' * 64),
])
def test_witness_rejects_scope_and_native_evidence_changes(captured, key, value):
    root, path, obs = captured
    obs[key] = value
    _write(path, obs)
    with pytest.raises(ValueError):
        auditor.audit(root, path)


def test_empty_date_menu_is_not_market_no_trade_proof(captured):
    root, path, obs = captured
    obs['date_labels'] = []
    _write(path, obs)
    proof = auditor.audit(root, path)
    assert proof['date_menu_contains_day'] is False
    assert 'not proof' in proof['limitations'][1]


@pytest.fixture
def registration(tmp_path, monkeypatch):
    monkeypatch.setattr(registrar, 'REPO', tmp_path)
    script = tmp_path / 'scripts' / 'tej_smart_wizard_bridge.ps1'
    script.parent.mkdir()
    script.write_text('fixture bridge', encoding='utf-8')
    root = tmp_path / 'data_tej'
    task = 'a' * 24
    attempt = task + '-' + 'b' * 32
    priority = 'c' * 64
    request = {'action': 'plan', 'frequency': 'daily', 'type': 'Option (ALL)',
               'smart_id': 'TEJ Derivatives', 'table': 'Delisted Option(2010~2013)',
               'start': '2011-01-01', 'end': '2011-12-31'}
    prepared = {**request, 'contract_version': registrar.CONTRACT_VERSION, 'task_id': task,
                'desktop_input_contract': registrar.DESKTOP_INPUT_CONTRACT}
    _write(root / 'requests' / (attempt + '.json'), prepared)
    snapshot_file = root / 'raw' / 'snapshot.json'
    _write(snapshot_file, {'fixture': True})
    snapshot = {'task_id': task, 'query_attempt_id': attempt, 'type': request['type'],
                'smart_id': request['smart_id'], 'table': request['table'],
                'source_proof': {'sha256': 'd' * 64, 'count': 510703, 'duplicate_exact_labels': 0},
                'snapshot_manifest_sha256': sha256_file(snapshot_file)}
    now = datetime.now(UTC)
    with sqlite3.connect(root / 'queue.sqlite3') as con:
        con.execute('CREATE TABLE tasks(task_id TEXT, kind TEXT, state TEXT)')
        con.execute('CREATE TABLE provider_query_intents(request_id TEXT, task_key TEXT, '
                    'created_at_utc TEXT, expires_at_utc TEXT)')
        con.execute('INSERT INTO tasks VALUES(?,?,?)', (task, 'discover', 'pending'))
        con.execute('INSERT INTO provider_query_intents VALUES(?,?,?,?)',
                    (priority, task, (now-timedelta(hours=1)).isoformat(),
                     (now+timedelta(days=1)).isoformat()))
    monkeypatch.setattr(registrar, 'audit_snapshot', lambda *a: snapshot)
    monkeypatch.setattr(registrar, 'candidate_labels', lambda *a: (['A', 'C'], 'e' * 64))
    monkeypatch.setattr(registrar, 'task_request', lambda *a, **kw: request)
    monkeypatch.setattr(registrar, 'inspect', lambda *a: {'provider': 'tej', 'priority_active': True,
                        'expires_at_utc': (now+timedelta(days=1)).isoformat()})
    monkeypatch.setattr(registrar, 'priority_dispatch_status', lambda *a, **k: {
        'state': 'admitted', 'available_bytes': 10_000_000, 'required_bytes': 1_000_000,
        'backing_volume': {'physical_free_bytes': 10_000_000, 'required_bytes': 1_000_000}})
    return root, snapshot_file, priority


def test_registration_is_one_shot_and_cannot_authorize_market_query(registration):
    root, snapshot, priority = registration
    result = registrar.register(root, snapshot, snapshot, priority, date(2011, 2, 16))
    body = json.loads(Path(result['intent_path']).read_text())
    assert body['market_data_query_authorized'] is False and body['company_labels'] == ['A', 'C']
    assert body['snapshot_manifest_path'] == str(snapshot.resolve())
    assert result['source_calls'] == 0 and result['source_data_admitted'] is False
    with pytest.raises(FileExistsError):
        registrar.register(root, snapshot, snapshot, priority, date(2011, 2, 16))


def test_registration_reuses_only_attested_unsent_failure(registration, monkeypatch, tmp_path):
    root, snapshot, priority = registration
    task = 'a' * 24
    previous = task + '-' + 'e' * 32
    native = root / 'requests' / (task + '-' + 'b' * 32 + '.json')
    previous_request = root / 'requests' / (previous + '.json')
    previous_request.write_bytes(native.read_bytes())
    request = json.loads(previous_request.read_text())
    bridge_sha = sha256_file(tmp_path / 'scripts/tej_smart_wizard_bridge.ps1')
    intent = {'contract': registrar.INTENT, **{k: request[k] for k in ('task_id', 'type', 'smart_id', 'table')},
              'prepared_request_sha256': sha256_file(previous_request), 'bridge_script_sha256': bridge_sha,
              'market_data_query_authorized': False, 'provider_queries_by_registration': 0}
    claim = root / 'finite_axis_capture_intents' / 'claimed' / (previous + '.json')
    _write(claim, intent)
    outcome = {**{k: request[k] for k in ('task_id', 'type', 'smart_id', 'table', 'action')},
               'contract_version': registrar.CONTRACT_VERSION, 'provider': 'tej_smart_wizard',
               'market_data_query_submission_possible': False,
               'error_code': 'metadata_preparation_failed_before_preview'}
    outcome_path = root / 'raw' / (previous + '.json.outcome.json')
    _write(outcome_path, outcome)
    monkeypatch.setattr(DesktopBridge, 'bound_script_release', lambda *a: (None, bridge_sha))
    result = registrar.register(root, snapshot, snapshot, priority, date(2011, 2, 16))
    assert Path(result['intent_path']).is_file() and claim.is_file() and outcome_path.is_file()
    assert result['source_calls'] == 0


def test_registration_defers_without_physical_headroom(registration, monkeypatch):
    root, snapshot, priority = registration
    monkeypatch.setattr(registrar, 'priority_dispatch_status', lambda *a, **k: {
        'state': 'admitted', 'available_bytes': 10_000_000, 'required_bytes': 1_000_000,
        'backing_volume': {'physical_free_bytes': 2_000_000, 'required_bytes': 1_000_000}})
    with pytest.raises(ValueError, match='headroom'):
        registrar.register(root, snapshot, snapshot, priority, date(2011, 2, 16))
    assert not (root / 'finite_axis_capture_intents').exists()


def test_registration_uses_bounded_priority_reserve_and_rejects_unknown(registration, monkeypatch):
    root, snapshot, priority = registration
    seen = []

    def capacity(path, *, additional_headroom_bytes):
        seen.append((path, additional_headroom_bytes))
        return {'state': 'not_enrolled'}

    monkeypatch.setattr(registrar, 'priority_dispatch_status', capacity)
    with pytest.raises(ValueError, match='headroom'):
        registrar.register(root, snapshot, snapshot, priority, date(2011, 2, 16))
    assert seen == [(root.resolve(), 24 * 1024 ** 3)]
    assert not (root / 'finite_axis_capture_intents').exists()


def test_registration_refuses_inactive_priority(registration, monkeypatch):
    root, snapshot, priority = registration
    monkeypatch.setattr(registrar, 'inspect', lambda *a: {'provider': 'tej', 'priority_active': False})
    with pytest.raises(ValueError, match='Active canonical'):
        registrar.register(root, snapshot, snapshot, priority, date(2011, 2, 16))
