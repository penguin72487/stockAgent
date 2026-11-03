from __future__ import annotations

from collections import Counter
from contextlib import closing
import json
from pathlib import Path
from datetime import UTC, date, datetime, timedelta

import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.tej_history import (DesktopBridge, company_code, configure_preview_planning,
                                    connect, normalize_period, run_one, task_request)
from downloader.tej_planning import build_plan, next_request
from downloader.tej_priority import CONTRACT, register_priority_requests
from scripts import audit_tej_finite_option_axis_capture as auditor
from scripts import build_tej_finite_option_priority as builder
from scripts import run_tej_finite_option_priority_after_capture as relay
from test_tej_planning import DynamicBridge, axes, registry as registry


def finite_plan(registry, tmp_path, monkeypatch):
    root, config = registry
    configure_preview_planning(root, config)
    with closing(connect(root)) as con:
        definition = dict(con.execute('SELECT * FROM tables').fetchone())
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='discover'").fetchone())
        base = task_request(root, task, connection=con)
    attempt = task['task_id'] + '-' + 'b' * 32
    companies, dates = axes()
    raw = root / 'raw' / (attempt + '.json.finite-axis.json')
    request_path = root / 'requests' / (attempt + '.json')
    claim = root / 'finite_axis_capture_intents' / 'claimed' / (attempt + '.json')
    for path in (raw, request_path, claim):
        path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(request_path, base)
    snap = root / 'raw' / 'fixture-snapshot.json'
    atomic_write_json(snap, {'fixture': True})
    candidate_data = tmp_path / 'candidate.bin'
    candidate_data.write_bytes(b'candidate')
    candidate_manifest = tmp_path / 'candidate.json'
    atomic_write_json(candidate_manifest, {'data_path': str(candidate_data),
                      'data_sha256': sha256_file(candidate_data),
                      'source_pins': {}, 'implementation_sha256': {}})
    release = root / 'bridge_releases' / 'fixture.ps1'
    release.parent.mkdir(exist_ok=True)
    release.write_text('fixture', encoding='utf-8')
    atomic_write_json(claim, {'candidate_manifest_path': str(candidate_manifest),
                      'candidate_manifest_sha256': sha256_file(candidate_manifest),
                      'snapshot_manifest_path': str(snap), 'snapshot_manifest_sha256': sha256_file(snap)})
    atomic_write_json(raw, {'company_labels': companies, 'date_labels': dates[:1],
                      'intent_sha256': sha256_file(claim), 'prepared_request_sha256': sha256_file(request_path),
                      'bridge_script_sha256': sha256_file(release),
                      'candidate_manifest_sha256': sha256_file(candidate_manifest)})
    proof = {'task_id': task['task_id'], 'query_attempt_id': attempt,
             'type': definition['query_type'], 'smart_id': definition['smart_id'], 'table': definition['name'],
             'market_observations_obtained': False, 'full_axis_admitted_to_plan': False,
             'training_input_admitted': False, 'source_key_mode': 2, 'date_menu_contains_day': True,
             'observation_sha256': sha256_file(raw), 'claimed_intent_sha256': sha256_file(claim)}
    monkeypatch.setattr(auditor, 'audit', lambda *a: proof)
    monkeypatch.setattr(DesktopBridge, 'bound_script_release', lambda *a: (release, sha256_file(release)))
    native = build_plan(base, companies, dates[:1], config, [])
    reference = {'path': str(raw), 'sha256': sha256_file(raw)}
    # Use two field batches from the exact finite native plan.
    items = []
    for ordinal in range(2):
        request = next_request(native, ordinal)
        items.append({'table_id': definition['table_id'], 'request': request, 'priority': -20,
                      'reason': 'finite source verification', 'discovery_sha256': reference['sha256'],
                      'finite_axis_observation': reference})
    files = (raw, request_path, claim, snap, candidate_manifest, candidate_data, release)
    plan = {'contract': CONTRACT, 'sources': [{'path': str(p), 'sha256': sha256_file(p)} for p in files],
            'requests': items}
    return root, plan, proof, tmp_path / 'finite-plan.json', tmp_path / 'finite-receipt.json'


def submit(batch):
    root, plan, _, plan_path, receipt_path = batch
    atomic_write_json(plan_path, plan)
    return register_priority_requests(root, plan_path, receipt_path)


def test_finite_builder_covers_each_verified_cell_and_field_once(registry, tmp_path, monkeypatch):
    batch = finite_plan(registry, tmp_path, monkeypatch)
    root, prior, proof, plan_path, receipt_path = batch
    monkeypatch.setattr(builder, 'audit', lambda *a: proof)
    observation = prior['requests'][0]['finite_axis_observation']['path']
    plan = builder.build(root, Path(observation))
    seen = Counter((field, company, period) for item in plan['requests']
                   for field in item['request']['fields']
                   for company in item['request']['company_labels']
                   for period in item['request']['date_labels'])
    assert len(seen) == 59 * 2 and set(seen.values()) == {1}
    assert plan['provider_queries_by_planning'] == 0 and plan['full_axis_admitted_to_plan'] is False
    receipt = submit((root, plan, proof, plan_path, receipt_path))
    assert len(receipt['tasks']) == 3


def test_finite_relay_registers_and_idempotently_reuses_the_native_work(registry, tmp_path, monkeypatch):
    root, plan, proof, _, _ = finite_plan(registry, tmp_path, monkeypatch)
    proof.update(date='2011-02-16', contract='tej_finite_company_date_axis_independent_readback_v1',
                 observation_path=plan['requests'][0]['finite_axis_observation']['path'])
    witness = tmp_path / 'verified-witness.json'
    atomic_write_json(witness, proof)
    monkeypatch.setattr(relay, 'audit', lambda *a: proof)
    monkeypatch.setattr(relay, 'build', lambda *a: plan)
    expires = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    monkeypatch.setattr(relay, 'inspect', lambda *a: {'provider': 'tej', 'priority_active': True,
                                                     'expires_at_utc': expires})
    assert relay.wait(root, witness, 'c' * 64, tmp_path, 0.01) == 0
    assert relay.wait(root, witness, 'c' * 64, tmp_path, 0.01) == 0
    source = json.loads((tmp_path / 'tej_option_finite_20110216_priority_source_receipt_v1.json').read_text())
    provider = json.loads((tmp_path / 'tej_option_finite_20110216_provider_receipt_v1.json').read_text())
    assert len(source['tasks']) == 2 and provider['priority_active'] is True
    with closing(connect(root)) as con:
        assert con.execute("SELECT count(*) FROM tasks WHERE kind='download'").fetchone()[0] == 2


def test_finite_relay_does_not_register_an_unverified_native_key(registry, tmp_path, monkeypatch):
    root, plan, proof, _, _ = finite_plan(registry, tmp_path, monkeypatch)
    proof.update(date='2011-02-16', contract='tej_finite_company_date_axis_independent_readback_v1',
                 source_key_mode=3, observation_path=plan['requests'][0]['finite_axis_observation']['path'])
    witness = tmp_path / 'verified-witness.json'
    atomic_write_json(witness, proof)
    monkeypatch.setattr(relay, 'audit', lambda *a: proof)
    monkeypatch.setattr(relay, 'inspect', lambda *a: {'provider': 'tej', 'priority_active': True})
    assert relay.wait(root, witness, 'c' * 64, tmp_path, 0.01) == 2
    with closing(connect(root)) as con:
        assert con.execute("SELECT count(*) FROM tasks WHERE kind='download'").fetchone()[0] == 0


def test_finite_relay_binds_the_requested_date(registry, tmp_path, monkeypatch):
    root, plan, proof, _, _ = finite_plan(registry, tmp_path, monkeypatch)
    proof.update(date='2014-12-11', contract='tej_finite_company_date_axis_independent_readback_v1',
                 observation_path=plan['requests'][0]['finite_axis_observation']['path'])
    witness = tmp_path / 'verified-2014-witness.json'
    atomic_write_json(witness, proof)
    monkeypatch.setattr(relay, 'audit', lambda *a: proof)
    monkeypatch.setattr(relay, 'build', lambda *a: plan)
    expires = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    monkeypatch.setattr(relay, 'inspect', lambda *a: {'provider': 'tej', 'priority_active': True,
                                                     'expires_at_utc': expires})
    with pytest.raises(ValueError, match='witness readback changed'):
        relay.wait(root, witness, 'c' * 64, tmp_path, 0.01, date(2011, 2, 16))
    assert relay.wait(root, witness, 'c' * 64, tmp_path, 0.01, date(2014, 12, 11)) == 0
    assert (tmp_path / 'tej_option_finite_20141211_provider_receipt_v1.json').is_file()


def test_verified_partial_axes_queue_finite_work_without_claiming_full_discovery(registry, tmp_path, monkeypatch):
    batch = finite_plan(registry, tmp_path, monkeypatch)
    root, _, _, _, _ = batch
    receipt = submit(batch)
    assert len(receipt['tasks']) == 2 and receipt['global_plan_changed'] is False
    with closing(connect(root)) as con:
        assert con.execute('SELECT discovery_path FROM tables').fetchone()[0] is None
        assert con.execute('SELECT count(*) FROM download_plans').fetchone()[0] == 0
    bridge = DynamicBridge()
    # Exercise the awkward but valid order: complete full discovery before
    # the already registered finite tasks. They must not be superseded.
    assert run_one(root, bridge, kind='discover') == 'completed_task'
    with closing(connect(root)) as con:
        assert con.execute("SELECT count(*) FROM tasks WHERE kind='download' AND state='pending'").fetchone()[0] == 2
    for _ in range(20):
        if run_one(root, bridge) == 'idle':
            break
    else:
        pytest.fail('Canonical owner did not complete the finite and lazy source scope')
    seen = Counter((field, company_code(company), normalize_period(period))
                   for request in bridge.requests if request['action'] == 'download'
                   for field in request['fields'] for company in request['company_labels']
                   for period in request['date_labels'])
    companies, dates = axes()
    assert len(seen) == 59 * len(companies) * len(dates) and set(seen.values()) == {1}
    with closing(connect(root)) as con:
        assert con.execute("SELECT sum(work_expected_rows) FROM tasks WHERE kind='download' AND state='complete'").fetchone()[0] == 30
        assert con.execute('SELECT work_grid_rows FROM tables').fetchone()[0] == 30


@pytest.mark.parametrize('fault', ['wrong_key_mode', 'date_absent', 'omitted_dependency',
                                   'changed_label', 'changed_field_partition', 'changed_source_sha'])
def test_unverified_finite_scope_never_mutates_queue(registry, tmp_path, monkeypatch, fault):
    batch = finite_plan(registry, tmp_path, monkeypatch)
    root, plan, proof, _, receipt_path = batch
    if fault == 'wrong_key_mode':
        proof['source_key_mode'] = 3
    elif fault == 'date_absent':
        proof['date_menu_contains_day'] = False
    elif fault == 'omitted_dependency':
        plan['sources'] = plan['sources'][:-1]
    elif fault == 'changed_label':
        plan['requests'][0]['request']['company_labels'] = ['other==>company']
    elif fault == 'changed_field_partition':
        plan['requests'][0]['request']['field_partition']['plan_fingerprint'] = 'f' * 64
    else:
        plan['sources'][0]['sha256'] = '0' * 64
    with pytest.raises(ValueError):
        submit(batch)
    assert not receipt_path.exists()
    with closing(connect(root)) as con:
        assert con.execute("SELECT count(*) FROM tasks WHERE kind='download'").fetchone()[0] == 0
